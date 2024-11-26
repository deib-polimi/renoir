use clap::Parser;
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};
use renoir::prelude::*;

#[derive(Parser)]
struct Options {
    #[clap(short, long)]
    brokers: String,
}

#[tokio::main]
async fn main() {
    let (config, args) = RuntimeConfig::from_args();

    let opt = Options::parse_from(args);

    config.spawn_remote_workers();
    let ctx = StreamContext::new(config);

    // Kafka Configuration
    // Consuming:
    let consumer = ClientConfig::new()
        .set("group.id", "asd")
        .set("bootstrap.servers", &opt.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Info)
        .create::<StreamConsumer>()
        .expect("Consumer creation failed");

    consumer.subscribe(&["test1"]).expect("kafka fail");

    ctx.stream_kafka(consumer).for_each(|x| println!("{x:?}"));

    // Producing:
    let producer = ClientConfig::new()
        .set("bootstrap.servers", &opt.brokers)
        .set("message.timeout.ms", "5000")
        .create::<FutureProducer>()
        .expect("Producer creation error");

    ctx.stream_par_iter(0..200)
        .map(|x| format!("{x:08X}"))
        .write_kafka(producer, "test1");

    ctx.execute().await;
}
