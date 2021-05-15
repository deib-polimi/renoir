use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

fn std_time_instant() {
    let start = std::time::Instant::now();
    let _elapsed = start.elapsed();
}

fn coarsetime_instant() {
    let start = coarsetime::Instant::now();
    let _elapsed = start.elapsed();
}

fn coarsetime_instant_lazy() {
    let start = coarsetime::Instant::now();
    let _elapsed = start.elapsed_since_recent();
}

fn coarse_time_benchmark(c: &mut Criterion) {
    let updater = coarsetime::Updater::new(10).start().unwrap();

    let mut group = c.benchmark_group("coarse_time");
    group.sample_size(100);
    group.warm_up_time(Duration::from_secs(3));
    group.bench_function("std::time::Instant", |b| b.iter(std_time_instant));
    group.bench_function("coarsetime::Instant", |b| b.iter(coarsetime_instant));
    group.bench_function("coarsetime::Instant-lazy", |b| {
        b.iter(coarsetime_instant_lazy)
    });
    group.finish();

    updater.stop().unwrap();
}

criterion_group!(benches, coarse_time_benchmark);
criterion_main!(benches);
