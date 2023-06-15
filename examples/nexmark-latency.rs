use clap::Parser;
use nexmark::config::NexmarkConfig;
use noir::operator::Operator;
use noir::operator::Timestamp;
use noir::prelude::*;
use noir::Replication;
use noir::Stream;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use nexmark::event::*;

const WATERMARK_INTERVAL: usize = 1 << 20;

#[allow(unused)]
fn timestamp_gen((_, e): &(SystemTime, Event)) -> Timestamp {
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

/// Query 2: Selection
///
/// ```text
/// SELECT Rstream(auction, price)
/// FROM Bid [NOW]
/// WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
/// ```
fn query2(events: Stream<(SystemTime, Event), impl Operator<(SystemTime, Event)> + 'static>) {
    events
        .filter_map(|(s, e)| {
            if let Event::Bid(b) = e {
                Some((s, b))
            } else {
                None
            }
        })
        // .shuffle()
        .filter(|(_, b)| b.auction % 123 == 0)
        .map(|(t, _)| t)
        // .replication(Replication::One)
        .for_each(|t| TRACK_POINT.get_or_init("q2").record(t.elapsed().unwrap()));
}

/// Query 3: Local Item Suggestion
///
/// ```text
/// SELECT Istream(P.name, P.city, P.state, A.id)
/// FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
/// WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
/// ```
fn query3(events: Stream<(SystemTime, Event), impl Operator<(SystemTime, Event)> + 'static>) {
    let mut routes = events
        .route()
        .add_route(|(_, e)| matches!(e, Event::Person(_)))
        .add_route(|(_, e)| matches!(e, Event::Auction(_)))
        .build()
        .into_iter();
    // WHERE P.state = `OR' OR P.state = `ID' OR P.state = `CA'
    let person = routes
        .next()
        .unwrap()
        .map(|(t, e)| (t, unwrap_person(e)))
        .filter(|(_, p)| p.state == "or" || p.state == "id" || p.state == "ca");
    // WHERE A.category = 10
    let auction = routes
        .next()
        .unwrap()
        .map(|(t, e)| (t, unwrap_auction(e)))
        .filter(|(_, a)| a.category == 10);
    person
        // WHERE A.seller = P.id
        .join(auction, |(_, p)| p.id, |(_, a)| a.seller)
        .drop_key()
        // SELECT person, auction.id
        .map(|((t0, p), (t1, a))| (t0.max(t1), p.name, p.city, p.state, a.id))
        .map(|(t, ..)| t)
        .replication(Replication::One)
        .for_each(|t| TRACK_POINT.get_or_init("q3").record(t.elapsed().unwrap()))
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
fn query5(events: Stream<(SystemTime, Event), impl Operator<(SystemTime, Event)> + 'static>) {
    let window_descr = EventTimeWindow::sliding(1_000, 100);
    let bid = events
        .filter_map(filter_bid)
        .add_timestamps(|(_, b)| b.date_time as i64, {
            let mut count = 0;
            move |_, ts| watermark_gen(ts, &mut count, WATERMARK_INTERVAL)
        });

    // count how bids in each auction, for every window
    let counts = bid
        .map(|(t, b)| (t, b.auction))
        .group_by(|(_, a)| *a)
        .map(|(_, (t, _))| t)
        .window(window_descr.clone())
        .fold((UNIX_EPOCH, 0), |(t, count), t1| {
            *t = t1.max(*t);
            *count += 1;
        })
        .unkey();
    counts
        .window_all(window_descr)
        .fold_first(|(max, (t, max_count)), (id, (t1, count))| {
            *t = t1.max(*t);
            if count > *max_count {
                *max = id;
                *max_count = count;
            }
        })
        .drop_key()
        .map(|(_k, (t, _count))| t)
        .for_each(|t| TRACK_POINT.get_or_init("q5").record(t.elapsed().unwrap()))
}

static TRACK_POINT: micrometer::TrackPoint = micrometer::TrackPoint::new_thread_local();

fn events(
    env: &mut StreamEnvironment,
    args: &Args,
) -> Stream<(SystemTime, Event), impl Operator<(SystemTime, Event)>> {
    env.stream_iter({
        let conf = NexmarkConfig {
            num_event_generators: 1,
            avg_auction_byte_size: 0,
            avg_bid_byte_size: 0,
            avg_person_byte_size: 0,
            first_rate: 10_000_000,
            next_rate: 10_000_000,
            ..Default::default()
        };
        nexmark::EventGenerator::new(conf).take(args.n).map(|e| {
            let start = SystemTime::now();
            (start, e)
        })
    })
    .batch_mode(BatchMode::adaptive(
        args.batch,
        std::time::Duration::from_micros(args.dt_us),
    ))
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
fn filter_bid((t, e): (SystemTime, Event)) -> Option<(SystemTime, Bid)> {
    match e {
        Event::Bid(x) => Some((t, x)),
        _ => None,
    }
}

#[derive(Parser)]
#[clap(name = "nexmark-latency")]
struct Args {
    n: usize,

    q: String,

    #[clap(short, long, default_value_t = 1024)]
    batch: usize,

    #[clap(short, long, default_value_t = 1000)]
    dt_us: u64,

    #[clap(short, long, default_value_t = 32 << 10)]
    watermark_interval: usize,
}

fn main() {
    env_logger::init();
    let (config, args) = EnvironmentConfig::from_args();

    let args = Args::parse_from(std::iter::once("nexmark-latency".into()).chain(args));
    let q = &args.q[..];

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();
    micrometer::start();

    match q {
        "2" => query2(events(&mut env, &args)),
        "3" => query3(events(&mut env, &args)),
        "5" => query5(events(&mut env, &args)),

        _ => panic!("Invalid query! {q}"),
    }

    let start = Instant::now();
    env.execute_blocking();
    println!("q{q}:elapsed:{:?}", start.elapsed());

    eprintln!("==================================================");
    micrometer::summary_grouped();
    micrometer::append_csv("/tmp/nexmark-latency.csv", "noir").unwrap();
}
