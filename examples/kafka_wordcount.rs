use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::str::FromStr;

use kstring::KString;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::{ClientConfig, Message};
use renoir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    env_logger::init();

    let (config, args) = RuntimeConfig::from_args();
    if args.len() != 2 {
        panic!("Pass the broker as an argument");
    }
    let broker = &args[1];

    config.spawn_remote_workers();
    let ctx = StreamContext::new(config);

    // Kafka Configuration
    // Consuming:
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("group.id", "0002")
        .set("auto.offset.reset", "earliest")
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Info);

    ctx.stream_kafka(
        consumer_config,
        &["renoir-wordcount"],
        Replication::Unlimited,
    )
    .flat_map(|m| Some(String::from_utf8(m.payload()?.to_owned()).unwrap()))
    .batch_mode(BatchMode::Single)
    // .batch_mode(BatchMode::adaptive(1024, std::time::Duration::from_millis(100)))
    .flat_map(|line| {
        line.split_whitespace()
            .map(|w| KString::from_str(w).unwrap())
            .collect::<Vec<_>>()
    })
    .group_by(|k| k.clone())
    .drop_key()
    .rich_map({
        let mut acc = HashMap::<KString, u64, BuildHasherDefault<wyhash::WyHash>>::default();
        move |w| {
            let count = acc.entry(w.clone()).or_default();
            *count += 1;
            (w, *count)
        }
    })
    .for_each(|t| println!("{t:?}"));

    ctx.execute_blocking();
}
