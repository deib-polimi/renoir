use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use noir::operator::source::IteratorSource;
use noir::BatchMode;
use noir::StreamEnvironment;

fn shuffle(dataset: &'static [u32]) {
    let mut env = StreamEnvironment::default();

    let source = IteratorSource::new(dataset.iter().cloned());
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .shuffle()
        .map(|n| n / 2)
        .shuffle()
        .map(|n| n.wrapping_add(1))
        .shuffle()
        .map(|n| n.wrapping_mul(2))
        .shuffle()
        .map(|n| n.wrapping_sub(42));
    stream.for_each(std::mem::drop);
    env.execute();
}

fn shuffle_benchmark(c: &mut Criterion) {
    let seed = b"rstream2 by edomora97 and mark03".to_owned();
    let r = &mut SmallRng::from_seed(seed);

    const DATASET_SIZE: usize = 100_000;
    let mut dataset: [u32; DATASET_SIZE] = [0; DATASET_SIZE];
    for item in dataset.iter_mut() {
        *item = r.gen();
    }

    let dataset = Box::leak(Box::new(dataset)) as &_;

    let mut group = c.benchmark_group("shuffle");
    group.throughput(Throughput::Bytes(
        (DATASET_SIZE * std::mem::size_of::<u32>()) as u64,
    ));
    group.bench_function("shuffle", |b| b.iter(|| shuffle(black_box(dataset))));
    group.finish();
}

criterion_group!(benches, shuffle_benchmark);
criterion_main!(benches);
