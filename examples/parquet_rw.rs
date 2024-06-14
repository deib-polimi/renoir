use std::path::PathBuf;

use arrow_schema::{Field, Schema, DataType};
use renoir::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Output {
    pub text: String,
    pub value: u32,
}

impl Output {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("value", DataType::UInt32, false),
        ])
    }
}


fn main() {
    let conf = RuntimeConfig::local(4).unwrap();

    let ctx = StreamContext::new(conf.clone());

    let target_path = PathBuf::from("target/");
    let dir_path = if target_path.is_dir() {
        target_path
    } else {
        let dir = tempfile::tempdir().unwrap();
        dir.path().to_path_buf()
    };

    eprintln!("Writing to {}", dir_path.display());

    // Write to multiple files in parallel
    let path = dir_path.clone();
    ctx.stream_par_iter(0..100u32)
        .map(|i| Output { value: i, text: format!("{i:08x}")})
        .write_parquet_seq(path, Output::schema());

    ctx.execute_blocking();

    // Write to a single file
    let ctx = StreamContext::new(conf.clone());
    let mut path = dir_path.clone();
    path.push("one.parquet");
    ctx.stream_par_iter(0..100u32)
        .map(|i| Output { value: i, text: format!("{i:08x}")})
        .write_parquet_one(path, Output::schema());

    ctx.execute_blocking();


    eprintln!("Reading from parquet is not supported yet.");

    // let ctx = StreamContext::new(conf);
    // let mut path = dir_path;
    // path.push("0001.csv");
    // ctx.stream_csv::<(i32, String)>(path)
    //     .for_each(|t| println!("{t:?}"));

    // ctx.execute_blocking();
}
