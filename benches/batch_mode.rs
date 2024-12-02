use std::ops::Range;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use renoir::operator::source::IteratorSource;
use renoir::BatchMode;
use renoir::RuntimeConfig;
use renoir::StreamContext;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn batch_mode(batch_mode: BatchMode, dataset: Range<u32>) {
    let config = RuntimeConfig::local(4).unwrap();
    let env = StreamContext::new(config);

    let source = IteratorSource::new(dataset.into_iter());
    let stream = env.stream(source).batch_mode(batch_mode).fold(0, |_, b| {
        std::hint::black_box(b);
    });
    let _result = stream.collect_vec();
    env.execute_blocking();
}

fn batch_mode_benchmark(c: &mut Criterion) {
    tracing_subscriber::fmt::init();
    const DATASET_SIZE: u32 = 5_000_000;
    let d = 0..DATASET_SIZE;

    let mut group = c.benchmark_group("batch_mode");
    group.sample_size(30);
    group.throughput(Throughput::Elements(DATASET_SIZE as u64));
    group.bench_function("fixed-64", |b| {
        b.iter(|| batch_mode(BatchMode::fixed(256), d.clone()))
    });
    group.bench_function("fixed-256", |b| {
        b.iter(|| batch_mode(BatchMode::fixed(256), d.clone()))
    });
    group.bench_function("fixed-1024", |b| {
        b.iter(|| batch_mode(BatchMode::fixed(1024), d.clone()))
    });
    group.bench_function("fixed-8192", |b| {
        b.iter(|| batch_mode(BatchMode::fixed(8192), d.clone()))
    });
    group.bench_function("adaptive-5ms", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::adaptive(1 << 20, Duration::from_millis(5)),
                d.clone(),
            )
        })
    });
    group.bench_function("adaptive-50ms", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::adaptive(1 << 20, Duration::from_millis(50)),
                d.clone(),
            )
        })
    });
    group.bench_function("adaptive-5s", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::adaptive(1 << 20, Duration::from_millis(5000)),
                d.clone(),
            )
        })
    });
    #[cfg(feature = "tokio")]
    group.bench_function("timed-5ms", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::timed(1 << 20, Duration::from_millis(5)),
                d.clone(),
            )
        })
    });
    #[cfg(feature = "tokio")]
    group.bench_function("timed-50ms", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::timed(1 << 20, Duration::from_millis(50)),
                d.clone(),
            )
        })
    });
    #[cfg(feature = "tokio")]
    group.bench_function("timed-1s", |b| {
        b.iter(|| {
            batch_mode(
                BatchMode::timed(1 << 20, Duration::from_millis(1000)),
                d.clone(),
            )
        })
    });
    group.bench_function("single", |b| {
        b.iter(|| batch_mode(BatchMode::single(), d.clone()))
    });
    group.finish();
}

criterion_group!(benches, batch_mode_benchmark);
criterion_main!(benches);
