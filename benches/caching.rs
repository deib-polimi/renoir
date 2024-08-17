mod common;

use common::renoir_bench_default;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mimalloc::MiMalloc;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use renoir::operator::cache::{BincodeCacheConfig, BincodeCacher, VecCacher};
use renoir::StreamContext;
use tempfile::tempdir;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn caching_benchmark(c: &mut Criterion) {
    let seed = b"rstream2 by edomora97 and mark03".to_owned();
    let r = &mut StdRng::from_seed(seed);

    const DATASET_SIZE: usize = 10_000_000;
    let mut dataset: [u32; DATASET_SIZE] = [0; DATASET_SIZE];
    for item in dataset.iter_mut() {
        *item = r.gen();
    }

    let mut group = c.benchmark_group("memory_caching");
    group.throughput(criterion::Throughput::Elements(DATASET_SIZE as u64));
    group.bench_function("baseline", |b| {
        renoir_bench_default(b, |ctx| {
            ctx.stream_par_iter(0..DATASET_SIZE).for_each(|x| {
                black_box(x);
            });
        })
    });

    let ctx = StreamContext::new_local();
    let cached = ctx
        .stream_par_iter(0..DATASET_SIZE)
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
        .stream_par_iter(0..DATASET_SIZE)
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
                .stream_par_iter(0..DATASET_SIZE)
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
        })
        
    });
    group.finish();
}

criterion_group!(benches, caching_benchmark);
criterion_main!(benches);
