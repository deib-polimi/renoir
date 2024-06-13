use std::path::PathBuf;

use apache_avro::AvroSchema;
use clap::Parser;
use renoir::{prelude::*, Replication};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
struct Options {
    #[clap(short, long)]
    input: Option<PathBuf>,

    #[clap(short, long)]
    output: PathBuf,
}

#[derive(Serialize, Deserialize, AvroSchema, Clone, Debug)]
struct InputType {
    s: String,
    num: u32,
}

fn main() {
    let (conf, args) = RuntimeConfig::from_args();
    let opts = Options::parse_from(args);
    conf.spawn_remote_workers();

    let ctx = StreamContext::new(conf.clone());

    let source = if let Some(input) = opts.input {
        ctx.stream_avro_file(Replication::One, input)
            .from_avro_value::<InputType>()
            .into_boxed()
    } else {
        ctx.stream_iter((0..100).map(|i| InputType {
            s: format!("{i:o}"),
            num: i,
        }))
        .into_boxed()
    };

    source
        .inspect(|e| eprintln!("{e:?}"))
        .repartition_by(Replication::Unlimited, |x| (x.num % 5).into())
        .map(|mut e| {
            e.num *= 2;
            e
        })
        .write_avro_seq(opts.output);

    ctx.execute_blocking();
}
