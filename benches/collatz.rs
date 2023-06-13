use std::time::Duration;

use common::NoirBenchBuilder;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use noir::BatchMode;
use noir::StreamEnvironment;

mod common;

fn bench_main(c: &mut Criterion) {
    let mut g = c.benchmark_group("wordcount-line");
    g.sample_size(30);
    g.warm_up_time(Duration::from_secs(3));
    g.measurement_time(Duration::from_secs(12));
    g.throughput(Throughput::Elements(1));
    g.bench_function("collatz", |b| {
        let builder = NoirBenchBuilder::new(
            StreamEnvironment::default,
            |n: u64, env: &mut StreamEnvironment| {
                env.stream_par_iter(0..n)
                    .batch_mode(BatchMode::fixed(1024))
                    .map(move |n| {
                        let mut c = 0;
                        let mut cur = n;
                        while c < 10000 {
                            if cur % 2 == 0 {
                                cur /= 2;
                            } else {
                                cur = cur * 3 + 1;
                            }
                            c += 1;
                            if cur <= 1 {
                                break;
                            }
                        }
                        (c, n)
                    })
                    .reduce_assoc(|a, b| a.max(b))
                    .collect::<Vec<_>>()
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });
    g.finish();
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
