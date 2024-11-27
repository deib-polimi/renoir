use clap::Parser;
use rdkafka::{config::RDKafkaLogLevel, ClientConfig, Message};
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
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("group.id", "asd")
        .set("bootstrap.servers", &opt.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Info);

    ctx.stream_kafka(consumer_config, &["test1"], Replication::Unlimited)
        .map(|m| {
            m.payload()
                .map(|p| String::from_utf8_lossy(p).to_string())
                .unwrap_or_default()
        })
        .for_each(|x| println!("{x:?}"));

    // Producing:
    let mut producer = ClientConfig::new();
    producer
        .set("bootstrap.servers", &opt.brokers)
        .set("message.timeout.ms", "5000");

    ctx.stream_par_iter(0..200)
        .map(|x| format!("{x:08X}"))
        .write_kafka(producer, "test1");

    ctx.execute().await;
}
