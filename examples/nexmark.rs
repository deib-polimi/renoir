use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use noir::operator::sink::StreamOutput;
use noir::operator::source::CsvSource;
use noir::operator::window::{CountWindow, EventTimeWindow};
use noir::operator::{Operator, Timestamp};
use noir::EnvironmentConfig;
use noir::Stream;
use noir::StreamEnvironment;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Auction {
    id: i64,
    item_name: String,
    description: String,
    initial_bid: i64,
    reserve: i64,
    date_time: Timestamp,
    expires: Timestamp,
    seller: i64,
    category: i64,
    extra: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bid {
    auction: i64,
    bidder: i64,
    price: i64,
    date_time: Timestamp,
    extra: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Person {
    id: i64,
    email_address: String,
    credit_card: String,
    city: String,
    state: String,
    date_time: Timestamp,
    extra: String,
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
fn query0(bid: Stream<Bid, impl Operator<Bid> + 'static>) -> StreamOutput<Vec<Bid>> {
    bid.collect_vec()
}

/// Query 1: Currency Conversion
///
/// ```text
/// SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
/// FROM bid [ROWS UNBOUNDED];
/// ```
fn query1(bid: Stream<Bid, impl Operator<Bid> + 'static>) -> StreamOutput<Vec<Bid>> {
    bid.map(|mut b| {
        b.price = (b.price as f32 * 0.908) as i64;
        b
    })
    .collect_vec()
}

/// Query 2: Selection
///
/// ```text
/// SELECT Rstream(auction, price)
/// FROM Bid [NOW]
/// WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
/// ```
fn query2(bid: Stream<Bid, impl Operator<Bid> + 'static>) -> StreamOutput<Vec<Bid>> {
    bid.filter(|b| {
        b.auction == 1007
            || b.auction == 1020
            || b.auction == 2001
            || b.auction == 2019
            || b.auction == 2007
    })
    .collect_vec()
}

/// Query 3: Local Item Suggestion
///
/// ```text
/// SELECT Istream(P.name, P.city, P.state, A.id)
/// FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
/// WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
/// ```
fn query3(
    auction: Stream<Auction, impl Operator<Auction> + 'static>,
    person: Stream<Person, impl Operator<Person> + 'static>,
) -> StreamOutput<Vec<(Person, i64)>> {
    // WHERE P.state = `OR' OR P.state = `ID' OR P.state = `CA'
    let person = person.filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");
    // WHERE A.category = 10
    let auction = auction.filter(|a| a.category == 10);
    person
        // WHERE A.seller = P.id
        .join(auction, |p| p.id, |a| a.seller)
        .drop_key()
        // SELECT person, auction.id
        .map(|(p, a)| (p, a.id))
        .collect_vec()
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
fn query4(
    auction: Stream<Auction, impl Operator<Auction> + 'static>,
    bid: Stream<Bid, impl Operator<Bid> + 'static>,
) -> StreamOutput<Vec<(i64, f64)>> {
    winning_bids(auction, bid)
        // GROUP BY category, AVG(price)
        .group_by_avg(|(auction, _)| auction.category, |(_, bid)| bid.price as f64)
        .collect_vec()
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
fn query5(bid: Stream<Bid, impl Operator<Bid> + 'static>) -> StreamOutput<Vec<i64>> {
    let window_descr = EventTimeWindow::sliding(
        Duration::from_secs(60 * 60), // 60 min
        Duration::from_secs(60),      // 1 min
    );
    // count how bids in each auction, for every window
    let counts = bid
        .group_by(|b| b.auction)
        .window(window_descr.clone())
        .map(|w| w.len())
        .unkey();
    counts
        .window_all(window_descr)
        // for every window: WHERE num >= ALL (...)
        .map(|w| {
            let window: Vec<_> = w.collect();
            let max_count = window.iter().map(|(_, c)| *c).max().expect("empty window");
            window
                .into_iter()
                .filter(|&&(_a, c)| c == max_count)
                .map(|&(a, _)| a)
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect_vec()
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
fn query6(
    auction: Stream<Auction, impl Operator<Auction> + 'static>,
    bid: Stream<Bid, impl Operator<Bid> + 'static>,
) -> StreamOutput<Vec<(i64, f32)>> {
    winning_bids(auction, bid)
        // [PARTITION BY A.seller ROWS 10]
        .group_by(|(a, _b)| a.seller)
        .window(CountWindow::sliding(10, 1))
        // AVG(Q.final)
        .map(|w| {
            let (sum, count) = w.fold((0, 0), |(s, c), (_a, b)| (s + b.price, c + 1));
            sum as f32 / count as f32
        })
        .collect_vec()
}

/// Query 7: Highest Bid
///
/// ```text
/// SELECT Rstream(B.auction, B.price, B.bidder)
/// FROM Bid [RANGE 1 MINUTE SLIDE 1 MINUTE] B
/// WHERE B.price = (SELECT MAX(B1.price)
///                  FROM BID [RANGE 1 MINUTE SLIDE 1 MINUTE] B1);
/// ```
fn query7(bid: Stream<Bid, impl Operator<Bid> + 'static>) -> StreamOutput<Vec<Bid>> {
    let window_descr = EventTimeWindow::tumbling(Duration::from_secs(60));
    bid.window_all(window_descr)
        .map(|w| {
            let window: Vec<_> = w.collect();
            let max_price = window.iter().map(|b| b.price).max().expect("empty window");
            window
                .into_iter()
                .filter(|b| b.price == max_price)
                .cloned()
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect_vec()
}

/// Query 8: Monitor New Users
///
/// ```text
/// SELECT Rstream(P.id, P.name, A.reserve)
/// FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
/// WHERE P.id = A.seller;
/// ```
fn query8(
    auction: Stream<Auction, impl Operator<Auction> + 'static>,
    person: Stream<Person, impl Operator<Person> + 'static>,
) -> StreamOutput<Vec<(Person, i64)>> {
    let window_descr = EventTimeWindow::tumbling(Duration::from_secs(60 * 60 * 12)); // 12h
    person
        .group_by(|p| p.id)
        .window(window_descr)
        .join(auction.group_by(|a| a.seller))
        .drop_key()
        .map(|(p, a)| (p, a.reserve))
        .collect_vec()
}

fn auctions(env: &mut StreamEnvironment, path: &Path) -> Stream<Auction, impl Operator<Auction>> {
    let path = path.join("auctions.csv");
    if !path.exists() {
        panic!("Missing auctions dataset at: {}", path.display());
    }
    env.stream(CsvSource::new(path)).add_timestamps(
        |x: &Auction| x.date_time,
        |x, &ts| if x.id % 10 == 0 { Some(ts) } else { None },
    )
}

fn persons(env: &mut StreamEnvironment, path: &Path) -> Stream<Person, impl Operator<Person>> {
    let path = path.join("persons.csv");
    if !path.exists() {
        panic!("Missing persons dataset at: {}", path.display());
    }
    env.stream(CsvSource::new(path)).add_timestamps(
        |x: &Person| x.date_time,
        |x, &ts| if x.id % 10 == 0 { Some(ts) } else { None },
    )
}

fn bids(env: &mut StreamEnvironment, path: &Path) -> Stream<Bid, impl Operator<Bid>> {
    let path = path.join("bids.csv");
    if !path.exists() {
        panic!("Missing bids dataset at: {}", path.display());
    }
    env.stream(CsvSource::new(path)).add_timestamps(
        |x: &Bid| x.date_time,
        |x, &ts| if x.auction % 10 == 0 { Some(ts) } else { None },
    )
}

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset directory as an argument");
    }
    let path = Path::new(&args[0]);
    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let q0 = query0(bids(&mut env, path));
    let q1 = query1(bids(&mut env, path));
    let q2 = query2(bids(&mut env, path));
    let q3 = query3(auctions(&mut env, path), persons(&mut env, path));
    let q4 = query4(auctions(&mut env, path), bids(&mut env, path));
    let q5 = query5(bids(&mut env, path));
    let q6 = query6(auctions(&mut env, path), bids(&mut env, path));
    let q7 = query7(bids(&mut env, path));
    let q8 = query8(auctions(&mut env, path), persons(&mut env, path));

    env.execute();

    eprintln!("Query0: {:?}", q0.get().map(|q| q.len()));
    eprintln!("Query1: {:?}", q1.get().map(|q| q.len()));
    eprintln!("Query2: {:?}", q2.get().map(|q| q.len()));
    eprintln!("Query3: {:?}", q3.get().map(|q| q.len()));
    eprintln!("Query4: {:?}", q4.get().map(|q| q.len()));
    eprintln!("Query5: {:?}", q5.get().map(|q| q.len()));
    eprintln!("Query6: {:?}", q6.get().map(|q| q.len()));
    eprintln!("Query7: {:?}", q7.get().map(|q| q.len()));
    eprintln!("Query8: {:?}", q8.get().map(|q| q.len()));
}
