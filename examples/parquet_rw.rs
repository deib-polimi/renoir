use std::path::PathBuf;

use arrow::datatypes::{DataType, Field, Float64Type, Schema, UInt32Type, Utf8Type};
use renoir::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Output {
    pub value: u32,
    pub root: f64,
    pub text: String,
}

impl Output {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("value", DataType::UInt32, false),
            Field::new("root", DataType::Float64, false),
            Field::new("text", DataType::Utf8, false),
        ])
    }
}

fn gen(i: u32) -> Output {
    Output {
        value: i,
        root: (i as f64).sqrt(),
        text: format!("{:x}", i * i * i),
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
        .map(gen)
        .write_parquet_seq(path, Output::schema());

    ctx.execute_blocking();

    // Write to a single file
    let ctx = StreamContext::new(conf.clone());
    let mut path = dir_path.clone();
    path.push("one.parquet");
    ctx.stream_par_iter(0..100u32)
        .map(gen)
        .write_parquet_one(path, Output::schema());

    ctx.execute_blocking();

    eprintln!("Reading from parquet");

    let ctx = StreamContext::new(conf);
    let mut path = dir_path;
    path.push("one.parquet");
    ctx.stream_parquet_one(path)
        .to_rows::<(UInt32Type, Float64Type, Utf8Type)>()
        .for_each(|t| println!("{t:?}"));

    ctx.execute_blocking();
}
