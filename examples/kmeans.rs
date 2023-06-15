use std::cmp::Ordering;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::ops::{AddAssign, Div};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
struct Point {
    x: f64,
    y: f64,
}

impl Point {
    fn distance_to(&self, other: &Point) -> f64 {
        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()
    }
}

impl AddAssign for Point {
    fn add_assign(&mut self, other: Self) {
        self.x += other.x;
        self.y += other.y;
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        let precision = 0.1;
        (self.x - other.x).abs() < precision && (self.y - other.y).abs() < precision
    }
}

impl Eq for Point {}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.x.partial_cmp(&other.x) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.y.partial_cmp(&other.y)
    }
}

impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.x.to_le_bytes().hash(state);
        self.y.to_le_bytes().hash(state);
    }
}

impl Div<f64> for Point {
    type Output = Self;

    fn div(self, rhs: f64) -> Self::Output {
        Self {
            x: self.x / rhs,
            y: self.y / rhs,
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
    *old_centroids
        .iter()
        .map(|c| (c, point.distance_to(c)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .unwrap()
        .0
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct State {
    iter_count: i64,
    old_centroids: Vec<Point>,
    centroids: Vec<Point>,
    changed: bool,
}

impl State {
    fn new(centroids: Vec<Point>) -> State {
        State {
            centroids,
            ..Default::default()
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

    let source = CsvSource::<Point>::new(path).has_headers(false);
    let res = env
        .stream(source)
        .replay(
            num_iters,
            initial_state,
            |s, state| {
                s.map(move |point| (point, select_nearest(point, &state.get().centroids), 1))
                    .group_by_avg(|(_p, c, _n)| *c, |(p, _c, _n)| *p)
                    .drop_key()
            },
            |update: &mut Vec<Point>, p| update.push(p),
            move |state, mut update| {
                if state.changed {
                    state.changed = true;
                    state.old_centroids.clear();
                    state.old_centroids.append(&mut state.centroids);
                }
                state.centroids.append(&mut update);
            },
            |state| {
                state.changed = false;
                state.iter_count += 1;
                state.centroids.sort_unstable();
                state.old_centroids.sort_unstable();
                state.centroids != state.old_centroids
            },
        )
        .collect_vec();

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();
    if let Some(res) = res.get() {
        let state = &res[0];
        eprintln!("Iterations: {}", state.iter_count);
        eprintln!("Output: {:?}", state.centroids.len());
    }
    eprintln!("Elapsed: {elapsed:?}");
}
