mod common;

use common::renoir_bench_default;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mimalloc::MiMalloc;

use renoir::operator::cache::{BincodeCacheConfig, BincodeCacher, VecCacher};
use renoir::operator::Operator;
use renoir::{BatchMode, Stream, StreamContext};
use tempfile::tempdir;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const BATCH_SIZE: usize = 4096;
const N_INTEGER: usize = 10_000_000;
const N_NEXMARK: usize = 1_000_000;

fn integer_caching(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_caching");
    group.throughput(criterion::Throughput::Elements(N_INTEGER as u64));
    group.bench_function("baseline", |b| {
        renoir_bench_default(b, |ctx| {
            ctx.stream_par_iter(0..N_INTEGER).for_each(|x| {
                black_box(x);
            });
        })
    });

    let ctx = StreamContext::new_local();
    let cached = ctx
        .stream_par_iter(0..N_INTEGER)
        .collect_cache::<VecCacher<_>>(());
    ctx.execute_blocking();

    group.bench_function("vec", move |b| {
        renoir_bench_default(b, |ctx| {
            let stream = cached.clone().stream_in(ctx);
            stream.for_each(|x| {
                black_box(x);
            });
        })
    });

    let ctx = StreamContext::new_local();
    let dir = tempdir().unwrap();
    let cached = ctx
        .stream_par_iter(0..N_INTEGER)
        .collect_cache::<BincodeCacher<_>>(BincodeCacheConfig {
            batch_size: 1024,
            path: dir.path().to_owned(),
        });
    ctx.execute_blocking();

    group.bench_function("bincode", move |b| {
        renoir_bench_default(b, |ctx| {
            let stream = cached.clone().stream_in(ctx);
            stream.for_each(|x| {
                black_box(x);
            });
        })
    });

    group.bench_function("bincode-rw-full", move |b| {
        b.iter(|| {
            let dir = tempdir().unwrap();
            let ctx = StreamContext::new_local();
            let cached = ctx
                .stream_par_iter(0..N_INTEGER)
                .collect_cache::<BincodeCacher<_>>(BincodeCacheConfig {
                    batch_size: 8192,
                    path: dir.path().to_owned(),
                });
            ctx.execute_blocking();
            let (ctx, stream) = cached.clone().stream();
            stream.for_each(|x| {
                black_box(x);
            });
            ctx.execute_blocking();
        });
    });
    group.finish();
}

fn nexmark_caching(c: &mut Criterion) {
    let mut group = c.benchmark_group("nexmark_caching");
    group.throughput(criterion::Throughput::Elements(N_NEXMARK as u64));

    fn events(ctx: &StreamContext) -> Stream<impl Operator<Out = nexmark::event::Event>> {
        ctx.stream_par_iter(move |i, n| {
            let conf = nexmark::config::NexmarkConfig {
                num_event_generators: n as usize,
                first_rate: 10_000_000,
                next_rate: 10_000_000,
                ..Default::default()
            };
            nexmark::EventGenerator::new(conf)
                .with_offset(i)
                .with_step(n)
                .take(N_NEXMARK / n as usize + (i < N_NEXMARK as u64 % n) as usize)
        })
        .batch_mode(BatchMode::fixed(BATCH_SIZE))
    }

    group.bench_function("baseline", |b| {
        renoir_bench_default(b, |ctx| {
            events(ctx).for_each(|x| {
                black_box(x);
            });
        })
    });

    let ctx = StreamContext::new_local();
    let cached = events(&ctx)
        .collect_cache::<VecCacher<_>>(());
    ctx.execute_blocking();

    group.bench_function("vec", move |b| {
        renoir_bench_default(b, |ctx| {
            let stream = cached.clone().stream_in(ctx);
            stream.for_each(|x| {
                black_box(x);
            });
        })
    });

    let ctx = StreamContext::new_local();
    let dir = tempdir().unwrap();
    let cached = events(&ctx)
        .collect_cache::<BincodeCacher<_>>(BincodeCacheConfig {
            batch_size: 1024,
            path: dir.into_path().to_owned(),
        });
    ctx.execute_blocking();

    group.bench_function("bincode", move |b| {
        renoir_bench_default(b, |ctx| {
            let stream = cached.clone().stream_in(ctx);
            stream.for_each(|x| {
                black_box(x);
            });
        })
    });

    group.bench_function("bincode-rw-full", move |b| {
        b.iter(|| {
            let dir = tempdir().unwrap();
            let ctx = StreamContext::new_local();
            let cached = events(&ctx)
                .collect_cache::<BincodeCacher<_>>(BincodeCacheConfig {
                    batch_size: 8192,
                    path: dir.path().to_owned(),
                });
            ctx.execute_blocking();
            let (ctx, stream) = cached.clone().stream();
            stream.for_each(|x| {
                black_box(x);
            });
            ctx.execute_blocking();
        });
    });
    group.finish();
}

fn caching_benchmark(c: &mut Criterion) {
    integer_caching(c);
    nexmark_caching(c);
}

criterion_group!(benches, caching_benchmark);
criterion_main!(benches);
