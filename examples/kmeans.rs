use rstream::config::EnvironmentConfig;
use rstream::environment::StreamEnvironment;
use rstream::operator::source;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::ops::{Add, Div};
use std::time::Instant;

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
struct Point {
    x: f64,
    y: f64,
}

impl Point {
    fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    #[inline(always)]
    fn distance_to(&self, other: &Point) -> f64 {
        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()
    }
}

impl Add for Point {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            x: self.x + other.x,
            y: self.y + other.y,
        }
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        // FIXME
        let precision = 1.5;
        (self.x - other.x).abs() < precision && (self.y - other.y).abs() < precision
    }
}

impl Eq for Point {}

impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.x < other.x {
            Ordering::Less
        } else if self.x > other.x {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.x.to_be_bytes().hash(state);
        self.y.to_be_bytes().hash(state);
    }
}

impl Div<u32> for Point {
    type Output = Self;

    fn div(self, rhs: u32) -> Self::Output {
        Self {
            x: self.x / rhs as f64,
            y: self.y / rhs as f64,
        }
    }
}

fn read_centroids(filename: &str, n: usize) -> Vec<Point> {
    let file = File::open(filename).unwrap();
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file)
        .into_deserialize::<Point>()
        .map(Result::unwrap)
        .take(n)
        .collect()
}

fn select_nearest(point: Point, old_centroids: &[Point]) -> Point {
    let mut selected_distance = f64::MAX;
    let mut selected_centroid = Point::new(0.0, 0.0);
    for centroid in old_centroids {
        let distance = point.distance_to(&centroid);
        if distance < selected_distance {
            selected_distance = distance;
            selected_centroid = *centroid;
        }
    }
    selected_centroid
}

#[derive(Clone, Serialize, Deserialize)]
struct State {
    iter_count: i64,
    old_centroids: Vec<Point>,
    centroids: Vec<Point>,
    finished_update: bool,
}

impl State {
    fn new(centroids: Vec<Point>) -> State {
        State {
            iter_count: 0,
            old_centroids: vec![],
            centroids,
            finished_update: true,
        }
    }
}

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 3 {
        panic!("Pass the number of centroid, the number of iterations and the dataset path as arguments");
    }
    let num_centroids: usize = args[0].parse().expect("Invalid number of centroids");
    let num_iters: usize = args[1].parse().expect("Invalid number of iterations");
    let path = &args[2];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let centroids = read_centroids(path, num_centroids);
    assert_eq!(centroids.len(), num_centroids);
    let initial_state = State::new(centroids);

    let source = source::CsvSource::<Point>::new(path, false);
    let res = env
        .stream(source)
        .replay(
            num_iters,
            initial_state,
            |s, state| {
                s.map(move |point| (point, select_nearest(point, &state.get().centroids), 1))
                    .group_by_reduce(
                        |(_p, c, _n)| *c,
                        |(p1, c, n1), (p2, _, n2)| (p1 + p2, c, n1 + n2),
                    )
                    .unkey()
                    .map(|(_, (p, _, n))| p / n)
            },
            |mut update: Vec<Point>, p| {
                update.push(p);
                update
            },
            move |mut state, mut update| {
                if state.finished_update {
                    state.finished_update = false;
                    state.old_centroids.clear();
                    state.old_centroids.append(&mut state.centroids);
                }
                state.centroids.append(&mut update);
                state
            },
            |state| {
                state.finished_update = true;
                state.iter_count += 1;
                state.centroids.sort_unstable();
                state.old_centroids.sort_unstable();
                state.centroids != state.old_centroids
            },
        )
        .collect_vec();

    let start = Instant::now();
    env.execute();
    let elapsed = start.elapsed();
    if let Some(res) = res.get() {
        let state = &res[0];
        eprintln!("Iterations: {}", state.iter_count);
        eprintln!("Output: {:?}", state.centroids.len());
    }
    eprintln!("Elapsed: {:?}", elapsed);
}
