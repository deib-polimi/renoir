#![allow(unused)]

use criterion::{black_box, Bencher};
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use noir::*;

pub const SAMPLES: usize = 20;
pub const WARM_UP: Duration = Duration::from_secs(3);
pub const DURATION: Duration = Duration::from_secs(10);

// pub struct NoirBenchBuilder<F, G, R>
// where
//     F: Fn() -> StreamEnvironment,
//     G: Fn(&mut StreamEnvironment) -> R,
// {
//     make_env: F,
//     make_network: G,
//     _result: PhantomData<R>,
// }

// impl<F, G, R> NoirBenchBuilder<F, G, R>
// where
//     F: Fn() -> StreamEnvironment,
//     G: Fn(&mut StreamEnvironment) -> R,
// {
//     pub fn new(make_env: F, make_network: G) -> Self {
//         Self {
//             make_env,
//             make_network,
//             _result: Default::default(),
//         }
//     }

//     pub fn bench(&self, n: u64) -> Duration {
//         let mut time = Duration::default();
//         for _ in 0..n {
//             let mut env = (self.make_env)();
//             let _result = (self.make_network)(&mut env);
//             let start = Instant::now();
//             env.execute();
//             time += start.elapsed();
//             black_box(_result);
//         }
//         time
//     }
// }

// pub fn noir_bench_default(b: &mut Bencher, logic: impl Fn(&mut StreamEnvironment)) {
//     let builder = NoirBenchBuilder::new(StreamEnvironment::default, logic);
//     b.iter_custom(|n| builder.bench(n));
// }
