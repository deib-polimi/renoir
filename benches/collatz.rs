use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use noir::BatchMode;
use noir::StreamEnvironment;

mod common;
use common::*;

fn bench_main(c: &mut Criterion) {
    let mut g = c.benchmark_group("wordcount-line");
    g.sample_size(SAMPLES);
    g.warm_up_time(WARM_UP);
    g.measurement_time(DURATION);

    for size in [0u32, 1_000, 1_000_000, 100_000_000] {
        g.throughput(Throughput::Elements(size as u64));
        g.bench_with_input(BenchmarkId::new("collatz", size), &size, |b, n| {
            b.iter(|| {
                let mut env = StreamEnvironment::default();
                env.stream_par_iter(0..*n)
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
                    .collect::<Vec<_>>();
                env.execute();
            });
        });
    }

    g.finish();
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
