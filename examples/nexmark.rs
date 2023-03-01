use noir::operator::Operator;
use noir::operator::Timestamp;
use noir::prelude::*;
use noir::Stream;
use std::time::Instant;

use nexmark::event::*;

const WATERMARK_INTERVAL: usize = 1024;
const BATCH_SIZE: usize = 4096;
const SECOND_MILLIS: i64 = 1_000;

fn timestamp_gen(e: &Event) -> Timestamp {
    e.timestamp() as i64
}

fn watermark_gen(e: &Event, ts: &Timestamp) -> Option<Timestamp> {
    let w = match e {
        Event::Person(x) => x.id % WATERMARK_INTERVAL == 0,
        Event::Auction(x) => x.id % WATERMARK_INTERVAL == 0,
        Event::Bid(x) => (x.auction ^ x.bidder ^ x.date_time as usize) % WATERMARK_INTERVAL == 0,
    };
    if w {
        Some(*ts)
    } else {
        None
    }
}

/// For each concluded auction, find its winning bid.
fn winning_bids(
    auction: Stream<Auction, impl Operator<Auction> + 'static>,
    bid: Stream<Bid, impl Operator<Bid> + 'static>,
) -> Stream<(Auction, Bid), impl Operator<(Auction, Bid)>> {
    auction
        // TODO: filter a.expires < CURRENT_TIME
        // WHERE A.id = B.auction
        .join(bid, |a| a.id, |b| b.auction)
        // WHERE B.datetime < A.expires
        .filter(|(_, (a, b))| {
            b.price >= a.reserve && (a.date_time..a.expires).contains(&b.date_time)
        })
        // find the bid with the maximum price
        .fold(
            (None, None),
            |(auc, win_bid): &mut (Option<Auction>, Option<Bid>), (a, bid)| {
                if auc.is_some() {
                    if win_bid.as_ref().unwrap().price < bid.price {
                        *win_bid = Some(bid);
                    }
                } else {
                    *auc = Some(a);
                    *win_bid = Some(bid);
                }
            },
        )
        .drop_key()
        .map(|(auction, bid)| (auction.unwrap(), bid.unwrap()))
}

/// Query 0: Passthrough
fn query0(events: Stream<Event, impl Operator<Event> + 'static>) {
    events.for_each(std::mem::drop)
}

/// Query 1: Currency Conversion
///
/// ```text
/// SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
/// FROM bid [ROWS UNBOUNDED];
/// ```
fn query1(events: Stream<Event, impl Operator<Event> + 'static>) {
    events
        .filter_map(filter_bid)
        .map(|mut b| {
            b.price = (b.price as f32 * 0.908) as usize;
            b
        })
        .for_each(std::mem::drop)
}

/// Query 2: Selection
///
/// ```text
/// SELECT Rstream(auction, price)
/// FROM Bid [NOW]
/// WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
/// ```
fn query2(events: Stream<Event, impl Operator<Event> + 'static>) {
    events
        .filter_map(filter_bid)
        .filter(|b| b.auction % 123 == 0)
        .for_each(std::mem::drop)
}

/// Query 3: Local Item Suggestion
///
/// ```text
/// SELECT Istream(P.name, P.city, P.state, A.id)
/// FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
/// WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
/// ```
fn query3(events: Stream<Event, impl Operator<Event> + 'static>) {
    let mut routes = events
        .route()
        .add_route(|e| matches!(e, Event::Person(_)))
        .add_route(|e| matches!(e, Event::Auction(_)))
        .build()
        .into_iter();
    // WHERE P.state = `OR' OR P.state = `ID' OR P.state = `CA'
    let person = routes
        .next()
        .unwrap()
        .map(unwrap_person)
        .filter(|p| p.state == "or" || p.state == "id" || p.state == "ca");
    // WHERE A.category = 10
    let auction = routes
        .next()
        .unwrap()
        .map(unwrap_auction)
        .filter(|a| a.category == 10);
    person
        // WHERE A.seller = P.id
        .join(auction, |p| p.id, |a| a.seller)
        .drop_key()
        // SELECT person, auction.id
        .map(|(p, a)| (p.name, p.city, p.state, a.id))
        // .for_each(|q| println!("{q:?}"))
        .for_each(std::mem::drop)
}

/// Query 4: Average Price for a Category
///
/// ```text
/// SELECT Istream(AVG(Q.final))
/// FROM Category C, (SELECT Rstream(MAX(B.price) AS final, A.category)
///                   FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
///                   WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
///                   GROUP BY A.id, A.category) Q
/// WHERE Q.category = C.id
/// GROUP BY C.id;
/// ```
fn query4(events: Stream<Event, impl Operator<Event> + 'static>) {
    let mut routes = events
        .route()
        .add_route(|e| matches!(e, Event::Auction(_)))
        .add_route(|e| matches!(e, Event::Bid(_)))
        .build()
        .into_iter();

    let auction = routes.next().unwrap().map(unwrap_auction);
    let bid = routes.next().unwrap().map(unwrap_bid);

    winning_bids(auction, bid)
        // GROUP BY category, AVG(price)
        .map(|(a, b)| (a.category, b.price))
        .group_by_avg(|(category, _)| *category, |(_, price)| *price as f64)
        .unkey()
        .for_each(std::mem::drop)
}

/// Query 5: Hot Items
///
/// ```text
/// SELECT Rstream(auction)
/// FROM (SELECT B1.auction, count(*) AS num
///       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
///       GROUP BY B1.auction)
/// WHERE num >= ALL (SELECT count(*)
///                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
///                   GROUP BY B2.auction);
/// ```
fn query5(events: Stream<Event, impl Operator<Event> + 'static>) {
    let window_descr = EventTimeWindow::sliding(10 * SECOND_MILLIS, 2 * SECOND_MILLIS);
    let bid = events
        .add_timestamps(timestamp_gen, watermark_gen)
        .filter_map(filter_bid);

    // count how bids in each auction, for every window
    let counts = bid
        .map(|b| b.auction)
        .group_by(|a| *a)
        .map(|_| ())
        .window(window_descr.clone())
        .map(|w| w.len())
        .unkey();
    counts
        .window_all(window_descr)
        .map(|w| *w.max_by_key(|(_, v)| *v).unwrap())
        .for_each(std::mem::drop)
}

/// Query 6: Average Selling Price by Seller
///
/// ```text
/// SELECT Istream(AVG(Q.final), Q.seller)
/// FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
///       FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
///       WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
///       GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
/// GROUP BY Q.seller;
/// ```
fn query6(events: Stream<Event, impl Operator<Event> + 'static>) {
    let mut routes = events
        .route()
        .add_route(|e| matches!(e, Event::Auction(_)))
        .add_route(|e| matches!(e, Event::Bid(_)))
        .build()
        .into_iter();
    // let person = event.pop().unwrap().filter_map(filter_person);
    let auction = routes.next().unwrap().map(unwrap_auction);
    let bid = routes.next().unwrap().map(unwrap_bid);
    winning_bids(auction, bid)
        // [PARTITION BY A.seller ROWS 10]
        .map(|(a, b)| (a.seller, b.price))
        .group_by(|(seller, _)| *seller)
        .window(CountWindow::sliding(10, 1))
        // AVG(Q.final)
        .map(|w| {
            let l = w.len();
            let sum: usize = w.map(|(_, price)| price).sum();
            sum as f32 / l as f32
        })
        .unkey()
        .for_each(std::mem::drop)
}

/// Query 7: Highest Bid
///
/// ```text
/// SELECT Rstream(B.auction, B.price, B.bidder)
/// FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
/// WHERE B.price = (SELECT MAX(B1.price)
///                  FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);
/// ```
fn query7(events: Stream<Event, impl Operator<Event> + 'static>) {
    let bid = events
        .add_timestamps(timestamp_gen, watermark_gen)
        .filter_map(filter_bid);
    let window_descr = EventTimeWindow::tumbling(10 * SECOND_MILLIS);
    bid.map(|b| (b.auction, b.price, b.bidder))
        .key_by(|_| ())
        .window(window_descr.clone())
        .map(|w| *w.max_by_key(|(_, price, _)| price).unwrap())
        .drop_key()
        .window_all(window_descr)
        .map(|w| *w.max_by_key(|(_, price, _)| price).unwrap())
        .for_each(std::mem::drop)
}

/// Query 8: Monitor New Users
///
/// ```text
/// SELECT Rstream(P.id, P.name, A.reserve)
/// FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
/// WHERE P.id = A.seller;
/// ```
fn query8(events: Stream<Event, impl Operator<Event> + 'static>) {
    let window_descr = EventTimeWindow::tumbling(10 * SECOND_MILLIS);

    let mut routes = events
        .add_timestamps(timestamp_gen, watermark_gen)
        .route()
        .add_route(|e| matches!(e, Event::Person(_)))
        .add_route(|e| matches!(e, Event::Auction(_)))
        .build()
        .into_iter();

    let person = routes
        .next()
        .unwrap()
        .map(unwrap_person)
        .map(|p| (p.id, p.name));
    let auction = routes
        .next()
        .unwrap()
        .map(unwrap_auction)
        .map(|a| (a.seller, a.reserve));

    person
        .group_by(|(id, _)| *id)
        .window(window_descr)
        .join(auction.group_by(|(seller, _)| *seller))
        .drop_key()
        .map(|((id, name), (_, reserve))| (id, name, reserve))
        .for_each(std::mem::drop)
}

fn events(env: &mut StreamEnvironment, tot: usize) -> Stream<Event, impl Operator<Event>> {
    env.stream_par_iter(move |i, n| {
        nexmark::EventGenerator::default()
            .with_offset(i)
            .with_step(n)
            .take(tot / n as usize + (i < tot as u64 % n) as usize)
    })
    .batch_mode(BatchMode::fixed(BATCH_SIZE))
}

fn unwrap_bid(e: Event) -> Bid {
    match e {
        Event::Bid(x) => x,
        _ => panic!("tried to unwrap wrong event type!"),
    }
}
fn unwrap_auction(e: Event) -> Auction {
    match e {
        Event::Auction(x) => x,
        _ => panic!("tried to unwrap wrong event type!"),
    }
}
fn unwrap_person(e: Event) -> Person {
    match e {
        Event::Person(x) => x,
        _ => panic!("tried to unwrap wrong event type!"),
    }
}
fn filter_bid(e: Event) -> Option<Bid> {
    match e {
        Event::Bid(x) => Some(x),
        _ => None,
    }
}
// fn filter_auction(e: Event) -> Option<Auction> {
//     match e {
//         Event::Auction(x) => Some(x),
//         _ => None,
//     }
// }
// fn filter_person(e: Event) -> Option<Person> {
//     match e {
//         Event::Person(x) => Some(x),
//         _ => None,
//     }
// }
// const N: usize = 100_000_000;

fn main() {
    env_logger::init();

    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 2 {
        panic!("Pass the element count as argument");
    }
    let n: usize = args[0].parse().unwrap();
    let i: usize = args[1].parse().unwrap();
    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    match i {
        0 => query0(events(&mut env, n)),
        1 => query1(events(&mut env, n)),
        2 => query2(events(&mut env, n)),
        3 => query3(events(&mut env, n)),
        4 => query4(events(&mut env, n)),
        5 => query5(events(&mut env, n)),
        6 => query6(events(&mut env, n)),
        7 => query7(events(&mut env, n)),
        8 => query8(events(&mut env, n)),
        _ => panic!("Invalid query! {i}"),
    }

    let start = Instant::now();
    env.execute();
    println!("q{i}:elapsed:{:?}", start.elapsed());

    // eprintln!("Query{i}: {:?}", q.get());
}
