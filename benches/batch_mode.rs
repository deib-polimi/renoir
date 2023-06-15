use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use noir::operator::source::IteratorSource;
use noir::BatchMode;
use noir::EnvironmentConfig;
use noir::StreamEnvironment;

fn batch_mode(batch_mode: BatchMode, dataset: &'static [u32]) {
    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);

    let source = IteratorSource::new(dataset.iter().cloned());
    let stream = env
        .stream(source)
        .batch_mode(batch_mode)
        .fold(0u32, |a, b| *a = a.wrapping_add(b));
    let _result = stream.collect_vec();
    env.execute_blocking();
}

fn batch_mode_benchmark(c: &mut Criterion) {
    let seed = b"rstream2 by edomora97 and mark03".to_owned();
    let r = &mut StdRng::from_seed(seed);

    const DATASET_SIZE: usize = 100_000;
    let mut dataset: [u32; DATASET_SIZE] = [0; DATASET_SIZE];
    for item in dataset.iter_mut() {
        *item = r.gen();
    }

    let dataset = Box::leak(Box::new(dataset)) as &_;

    let mut group = c.benchmark_group("batch_mode");
    group.throughput(Throughput::Bytes(
        (DATASET_SIZE * std::mem::size_of::<u32>()) as u64,
    ));
    group.bench_function("fixed", |b| {
        b.iter(|| batch_mode(BatchMode::fixed(1024), black_box(dataset)))
    });
    group.bench_function("adaptive-5ms", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::adaptive(1024, Duration::from_millis(5)),
                black_box(dataset),
            )
        })
    });
    group.bench_function("adaptive-50ms", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::adaptive(1024, Duration::from_millis(50)),
                black_box(dataset),
            )
        })
    });
    group.bench_function("adaptive-5s", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::adaptive(1024, Duration::from_millis(5000)),
                black_box(dataset),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, batch_mode_benchmark);
criterion_main!(benches);
