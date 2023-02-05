use std::time::Duration;

use noir::operator::Operator;
use noir::prelude::*;
use noir::Stream;
use std::time::Instant;

use nexmark::event::*;

const WATERMARK_INTERVAL: usize = 256;

fn timestamp_gen(e: &Event) -> Duration {
    Duration::from_millis(e.timestamp())
}

fn watermark_gen(e: &Event, ts: &Duration) -> Option<Duration> {
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
        .filter(|(_, (a, b))| b.date_time < a.expires)
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
fn query0(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    events.collect_count()
}

/// Query 1: Currency Conversion
///
/// ```text
/// SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
/// FROM bid [ROWS UNBOUNDED];
/// ```
fn query1(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    events
        .filter_map(filter_bid)
        .map(|mut b| {
            b.price = (b.price as f32 * 0.908) as usize;
            b
        })
        .collect_count()
}

/// Query 2: Selection
///
/// ```text
/// SELECT Rstream(auction, price)
/// FROM Bid [NOW]
/// WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
/// ```
fn query2(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    events
        .filter_map(filter_bid)
        .filter(|b| {
            b.auction == 1007
                || b.auction == 1020
                || b.auction == 2001
                || b.auction == 2019
                || b.auction == 2007
        })
        .collect_count()
}

/// Query 3: Local Item Suggestion
///
/// ```text
/// SELECT Istream(P.name, P.city, P.state, A.id)
/// FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
/// WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
/// ```
fn query3(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    let mut events = events
        .filter(|e| matches!(e.event_type(), EventType::Auction | EventType::Person))
        .split(2);
    // WHERE P.state = `OR' OR P.state = `ID' OR P.state = `CA'
    let person = events
        .pop()
        .unwrap()
        .filter_map(filter_person)
        .filter(|p| p.state == "or" || p.state == "id" || p.state == "ca");
    // WHERE A.category = 10
    let auction = events
        .pop()
        .unwrap()
        .filter_map(filter_auction)
        .filter(|a| a.category == 10);
    person
        // WHERE A.seller = P.id
        .join(auction, |p| p.id, |a| a.seller)
        .drop_key()
        // SELECT person, auction.id
        .map(|(p, a)| (p.name, p.city, p.state, a.id))
        .collect_count()
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
fn query4(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    let mut events = events
        .filter(|e| matches!(e.event_type(), EventType::Auction | EventType::Bid))
        .split(2);

    let auction = events.pop().unwrap().filter_map(filter_auction);
    let bid = events.pop().unwrap().filter_map(filter_bid);

    winning_bids(auction, bid)
        // GROUP BY category, AVG(price)
        .group_by_avg(|(auction, _)| auction.category, |(_, bid)| bid.price as f64)
        .unkey()
        .collect_count()
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
fn query5(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    let window_descr = EventTimeWindow::sliding(
        Duration::from_secs(60 * 60), // 60 min
        Duration::from_secs(60),      // 1 min
    );
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
        // for every window: WHERE num >= ALL (...)
        .map(|w| w.max_by_key(|(_, v)| *v).unwrap().0)
        .collect_count()
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
fn query6(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    let mut events = events
        .filter(|e| matches!(e.event_type(), EventType::Auction | EventType::Bid))
        .split(2);
    // let person = event.pop().unwrap().filter_map(filter_person);
    let auction = events.pop().unwrap().filter_map(filter_auction);
    let bid = events.pop().unwrap().filter_map(filter_bid);
    winning_bids(auction, bid)
        // [PARTITION BY A.seller ROWS 10]
        .group_by(|(a, _b)| a.seller)
        .window(CountWindow::sliding(10, 1))
        // AVG(Q.final)
        .map(|w| {
            let l = w.len();
            let sum: usize = w.map(|(_, b)| b.price).sum();
            sum as f32 / l as f32
        })
        .unkey()
        .collect_count()
}

/// Query 7: Highest Bid
///
/// ```text
/// SELECT Rstream(B.auction, B.price, B.bidder)
/// FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
/// WHERE B.price = (SELECT MAX(B1.price)
///                  FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);
/// ```
fn query7(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    let bid = events
        .add_timestamps(timestamp_gen, watermark_gen)
        .filter_map(filter_bid);
    let window_descr = EventTimeWindow::tumbling(Duration::from_secs(60));
    bid.key_by(|_| ())
        .window(window_descr.clone())
        .map(|w| w.max_by_key(|b| b.price).unwrap().clone())
        .drop_key()
        .window_all(window_descr)
        .map(|w| w.max_by_key(|b| b.price).unwrap().clone())
        .collect_count()
}

/// Query 8: Monitor New Users
///
/// ```text
/// SELECT Rstream(P.id, P.name, A.reserve)
/// FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
/// WHERE P.id = A.seller;
/// ```
fn query8(events: Stream<Event, impl Operator<Event> + 'static>) -> StreamOutput<usize> {
    let window_descr = EventTimeWindow::tumbling(Duration::from_secs(60 * 60 * 12)); // 12h

    let mut events = events
        .filter(|e| matches!(e.event_type(), EventType::Auction | EventType::Person))
        .add_timestamps(timestamp_gen, watermark_gen)
        .split(2);
    let person = events.pop().unwrap().filter_map(filter_person);
    let auction = events.pop().unwrap().filter_map(filter_auction);

    person
        .group_by(|p| p.id)
        .window(window_descr)
        .join(auction.group_by(|a| a.seller))
        .drop_key()
        .map(|(p, a)| (p, a.reserve))
        .collect_count()
}

fn events(env: &mut StreamEnvironment, tot: usize) -> Stream<Event, impl Operator<Event>> {
    env.stream_par_iter(move |i, n| {
        nexmark::EventGenerator::default()
            .with_offset(i)
            .with_step(n)
            .take(tot / n as usize + (i < tot as u64 % n) as usize)
    })
    .batch_mode(BatchMode::fixed(1024))
}

fn filter_bid(e: Event) -> Option<Bid> {
    match e {
        Event::Bid(x) => Some(x),
        _ => None,
    }
}
fn filter_auction(e: Event) -> Option<Auction> {
    match e {
        Event::Auction(x) => Some(x),
        _ => None,
    }
}
fn filter_person(e: Event) -> Option<Person> {
    match e {
        Event::Person(x) => Some(x),
        _ => None,
    }
}
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

    let q = match i {
        0 => query0(events(&mut env, n)),
        1 => query1(events(&mut env, n)),
        2 => query2(events(&mut env, n)),
        3 => query3(events(&mut env, n)),
        4 => query4(events(&mut env, n)),
        5 => query5(events(&mut env, n)),
        6 => query6(events(&mut env, n)),
        7 => query7(events(&mut env, n)),
        8 => query8(events(&mut env, n)),
        _ => panic!("Invalid query"),
    };

    let start = Instant::now();
    env.execute();
    println!("elapsed: {:?}", start.elapsed());

    eprintln!("Query{i}: {:?}", q.get());
}
