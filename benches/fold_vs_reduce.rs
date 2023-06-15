use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use noir::operator::source::IteratorSource;
use noir::BatchMode;
use noir::EnvironmentConfig;
use noir::StreamEnvironment;

fn fold(dataset: &'static [u32]) {
    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);

    let source = IteratorSource::new(dataset.iter().cloned());
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .fold(0u32, |a, b| *a = a.wrapping_add(b));
    let _result = stream.collect_vec();
    env.execute_blocking();
}

fn reduce(dataset: &'static [u32]) {
    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);

    let source = IteratorSource::new(dataset.iter().cloned());
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .reduce(|a, b| a.wrapping_add(b));
    let _result = stream.collect_vec();
    env.execute_blocking();
}

fn fold_assoc(dataset: &'static [u32]) {
    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);

    let source = IteratorSource::new(dataset.iter().cloned());
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            0u32,
            |a, b| *a = a.wrapping_add(b),
            |a, b| *a = a.wrapping_add(b),
        );
    let _result = stream.collect_vec();
    env.execute_blocking();
}

fn reduce_assoc(dataset: &'static [u32]) {
    let config = EnvironmentConfig::local(4);
    let mut env = StreamEnvironment::new(config);

    let source = IteratorSource::new(dataset.iter().cloned());
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .reduce_assoc(|a, b| a.wrapping_add(b));
    let _result = stream.collect_vec();
    env.execute_blocking();
}

fn fold_vs_reduce_benchmark(c: &mut Criterion) {
    let seed = b"rstream2 by edomora97 and mark03".to_owned();
    let r = &mut StdRng::from_seed(seed);

    const DATASET_SIZE: usize = 100_000;
    let mut dataset: [u32; DATASET_SIZE] = [0; DATASET_SIZE];
    for item in dataset.iter_mut() {
        *item = r.gen();
    }

    let dataset = Box::leak(Box::new(dataset)) as &_;

    let mut group = c.benchmark_group("fold_vs_reduce");
    group.throughput(Throughput::Bytes(
        (DATASET_SIZE * std::mem::size_of::<u32>()) as u64,
    ));
    group.bench_function("fold", |b| b.iter(|| fold(black_box(dataset))));
    group.bench_function("fold-assoc", |b| b.iter(|| fold_assoc(black_box(dataset))));
    group.bench_function("reduce", |b| b.iter(|| reduce(black_box(dataset))));
    group.bench_function("reduce-assoc", |b| {
        b.iter(|| reduce_assoc(black_box(dataset)))
    });
    group.finish();
}

criterion_group!(benches, fold_vs_reduce_benchmark);
criterion_main!(benches);
