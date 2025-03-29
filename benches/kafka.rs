use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use once_cell::sync::Lazy;
use rand::{prelude::*, rng};
use rdkafka::ClientConfig;
use renoir::prelude::*;
use std::time::Duration;

mod common;
use common::*;

static BROKERS: Lazy<String> = Lazy::new(|| {
    std::env::var("RENOIR_KAFKA_BROKER").expect("Must specify kafka broker in RENOIR_KAFKA_BROKER")
});

fn kafka_prod(ctx: &StreamContext, size: u64, topic: &str) {
    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", BROKERS.as_str())
        .set("message.timeout.ms", "5000");
    ctx.stream_par_iter(0u64..size)
        .batch_mode(BatchMode::timed(1024, Duration::from_millis(100)))
        .map(|x| x.to_ne_bytes())
        .write_kafka(producer, topic);
}

fn kafka_consume(ctx: &StreamContext, size: u64, topic: &str) {
    let mut consumer_config = ClientConfig::new();

    let g = format!("{:010x}", rng().random_range(0..100000));
    consumer_config
        .set("group.id", g)
        .set("bootstrap.servers", BROKERS.as_str())
        // .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "earliest");

    ctx.stream_kafka(consumer_config, &[topic], Replication::One)
        .limit(size as usize)
        .batch_mode(BatchMode::timed(1024, Duration::from_millis(100)))
        // .inspect(|x| eprintln!("dbg: {x:?}"))
        .map(|msg| {
            std::hint::black_box(msg);
        })
        .repartition_by(Replication::One, |_| 0)
        .collect_count();
}

fn bench_main(c: &mut Criterion) {
    tracing_subscriber::fmt::init();
    let mut g = c.benchmark_group("kafka");
    g.sample_size(SAMPLES);
    // g.warm_up_time(WARM_UP);
    // g.measurement_time(DURATION);

    for size in [0u64, 1_000, 1_000_000] {
        g.throughput(Throughput::Elements(size));

        g.bench_with_input(BenchmarkId::new("produce", size), &size, |b, size| {
            renoir_bench_default_tokio(b, move |ctx| {
                kafka_prod(ctx, *size, "renoir-kafka-t1");
            });
        });
    }

    for size in [10, 1_000, 1_000_000] {
        g.throughput(Throughput::Elements(size));

        g.bench_with_input(
            BenchmarkId::new("produce-consume", size),
            &size,
            |b, size| {
                renoir_bench_default_tokio(b, move |ctx| {
                    kafka_consume(ctx, *size, "renoir-kafka-t1");
                    kafka_prod(ctx, *size, "renoir-kafka-t1");
                });
            },
        );
    }

    g.finish();
}

criterion_group!(benches, bench_main);
criterion_main!(benches);
