use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use nexmark::config::NexmarkConfig;
use noir::operator::window::TransactionOp;
use noir::operator::window::TransactionWindow;
use noir::operator::Operator;
use noir::operator::Timestamp;
use noir::prelude::*;
use noir::Stream;

use nexmark::event::*;

mod common;
use common::*;

const WATERMARK_INTERVAL: usize = 1 << 20;
const BATCH_SIZE: usize = 16 << 10;
const SECOND_MILLIS: i64 = 1_000;

fn timestamp_gen(e: &Event) -> Timestamp {
    e.timestamp() as i64
}

fn watermark_gen(ts: &Timestamp, count: &mut usize, interval: usize) -> Option<Timestamp> {
    *count = (*count + 1) % interval;
    if *count == 0 {
        Some(*ts)
    } else {
        None
    }
}

/// For each concluded auction, find its winning bid.
fn winning_bids(
    events: Stream<Event, impl Operator<Event> + 'static>,
) -> Stream<(Auction, Bid), impl Operator<(Auction, Bid)>> {
    events
        .filter(|e| matches!(e, Event::Auction(_) | Event::Bid(_)))
        .add_timestamps(timestamp_gen, {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
        .group_by(|e| match e {
            Event::Auction(a) => a.id,
            Event::Bid(b) => b.auction,
            _ => unreachable!(),
        })
        .window(TransactionWindow::new(|e| match e {
            Event::Auction(a) => TransactionOp::CommitAfter(a.expires as i64),
            _ => TransactionOp::Continue,
        }))
        // find the bid with the maximum price
        .fold(
            (None, Vec::new()),
            |(auction, bids): &mut (Option<Auction>, Vec<Bid>), e| match e {
                Event::Auction(a) => {
                    let winner = bids
                        .drain(..)
                        .filter(|b| {
                            b.price >= a.reserve && (a.date_time..a.expires).contains(&b.date_time)
                        })
                        .max_by_key(|b| b.price);

                    *auction = Some(a);
                    bids.extend(winner);
                    bids.shrink_to(1);
                }
                Event::Bid(b) => {
                    if let Some(a) = auction {
                        if b.price >= a.reserve
                            && (a.date_time..a.expires).contains(&b.date_time)
                            && bids.first().map(|w| b.price > w.price).unwrap_or(true)
                        {
                            bids.truncate(0);
                            bids.push(b);
                        }
                    } else {
                        // Save all out of order since we cannot filter yet
                        bids.push(b);
                    }
                }
                _ => unreachable!(),
            },
        )
        .drop_key()
        .filter_map(|(auction, mut bid)| Some((auction.unwrap(), bid.pop()?)))
    // .inspect(|e| eprintln!("{e:?}"))
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
    winning_bids(events)
        // GROUP BY category, AVG(price)
        .map(|(a, b)| (a.category, b.price))
        // .inspect(|a| println!("{a:?}"))
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
        .add_timestamps(timestamp_gen, {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
        .filter_map(filter_bid);

    // count how bids in each auction, for every window
    let counts = bid
        .map(|b| b.auction)
        .group_by(|a| *a)
        .map(|_| ())
        .window(window_descr.clone())
        .count()
        .unkey();
    counts
        .window_all(window_descr)
        .max_by_key(|(_, v)| *v)
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
    winning_bids(events)
        // [PARTITION BY A.seller ROWS 10]
        .map(|(a, b)| (a.seller, b.price))
        .group_by(|(seller, _)| *seller)
        .window(CountWindow::sliding(10, 1))
        // AVG(Q.final)
        .fold((0, 0), |(sum, count), (_, price)| {
            *sum += price;
            *count += 1;
        })
        .map(|(_, (sum, count))| sum as f32 / count as f32)
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
        .add_timestamps(timestamp_gen, {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
        .filter_map(filter_bid);
    let window_descr = EventTimeWindow::tumbling(10 * SECOND_MILLIS);
    bid.map(|b| (b.auction, b.price, b.bidder))
        .key_by(|_| ())
        .window(window_descr.clone())
        .max_by_key(|(_, price, _)| *price)
        .drop_key()
        .window_all(window_descr)
        .max_by_key(|(_, price, _)| *price)
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
        .add_timestamps(timestamp_gen, {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        })
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
        .window_join(window_descr, auction.group_by(|(seller, _)| *seller))
        .drop_key()
        .map(|((id, name), (_, reserve))| (id, name, reserve))
        .for_each(std::mem::drop)
}

fn events(env: &mut StreamEnvironment, tot: usize) -> Stream<Event, impl Operator<Event>> {
    env.stream_par_iter(move |i, n| {
        let conf = NexmarkConfig {
            num_event_generators: n as usize,
            first_rate: 10_000_000,
            next_rate: 10_000_000,
            ..Default::default()
        };
        nexmark::EventGenerator::new(conf)
            .with_offset(i)
            .with_step(n)
            .take(tot / n as usize + (i < tot as u64 % n) as usize)
    })
    .batch_mode(BatchMode::fixed(BATCH_SIZE))
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

fn run_query(env: &mut StreamEnvironment, q: &str, n: usize) {
    match q {
        "0" => query0(events(env, n)),
        "1" => query1(events(env, n)),
        "2" => query2(events(env, n)),
        "3" => query3(events(env, n)),
        "4" => query4(events(env, n)),
        "5" => query5(events(env, n)),
        "6" => query6(events(env, n)),
        "7" => query7(events(env, n)),
        "8" => query8(events(env, n)),

        _ => panic!("Invalid query! {q}"),
    }
}

fn bench_main(c: &mut Criterion) {
    let mut g = c.benchmark_group("nexmark");
    g.sample_size(SAMPLES);
    g.warm_up_time(WARM_UP);
    g.measurement_time(DURATION);

    macro_rules! bench_query {
        ($q:expr, $n:expr) => {{
            g.bench_with_input(BenchmarkId::new($q, $n), &$n, |b, size| {
                b.iter(|| {
                    let mut env = StreamEnvironment::default();
                    run_query(&mut env, $q, *size);
                    env.execute();
                })
            });
        }};
    }

    for size in [1_000, 100_000, 1_000_000] {
        g.throughput(Throughput::Elements(size as u64));
        bench_query!("0", size);
        bench_query!("1", size);
        bench_query!("2", size);
        bench_query!("3", size);
        bench_query!("4", size);
        bench_query!("5", size);
        bench_query!("6", size);
        bench_query!("7", size);
        bench_query!("8", size);
    }

    g.finish();
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
