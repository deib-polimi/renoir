use apache_avro::{AvroSchema, Schema, Writer};
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::block::NextStrategy;
use crate::operator::{ExchangeData, Operator};
use crate::scheduler::ExecutionMetadata;
use crate::{CoordUInt, Replication, Stream};

use super::writer::{sequential_path, WriteOperator, WriterOperator};

// #[derive(Debug)]
pub struct AvroSink<T> {
    _t: PhantomData<T>,
    writer: Option<BufWriter<File>>,
    schema: Schema,
}

impl<T> AvroSink<T>
where
    T: AvroSchema + Serialize,
{
    pub fn new() -> Self {
        Self {
            _t: PhantomData,
            writer: None,
            schema: T::get_schema(),
        }
    }
}

impl<T: Serialize> WriteOperator<T> for AvroSink<T>
where
    T: AvroSchema + Serialize + Send,
{
    type Destination = PathBuf;

    fn write(&mut self, items: &mut impl Iterator<Item = T>) {
        let w = self.writer.as_mut().unwrap();
        let mut w = Writer::new(&self.schema, w);
        for item in items {
            w.append_ser(item).expect("failed to write to avro");
        }
        w.flush().unwrap();
    }

    fn flush(&mut self) { /* already flushes in write */
    }

    fn finalize(&mut self) {
        if let Some(mut w) = self.writer.take() {
            w.flush().unwrap();
        }
    }

    fn setup(&mut self, destination: Self::Destination) {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&destination)
            .unwrap_or_else(|err| {
                panic!(
                    "AvroSource: error while opening file {:?}: {:?}",
                    destination, err
                )
            });

        let buf_writer = BufWriter::new(file);
        self.writer = Some(buf_writer);
    }
}

impl<T> Clone for AvroSink<T> {
    fn clone(&self) -> Self {
        Self {
            _t: PhantomData,
            writer: None,
            schema: self.schema.clone(),
        }
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: AvroSchema + Serialize,
{
    pub fn write_avro<F: FnOnce(CoordUInt) -> PathBuf + Clone + Send + 'static>(
        self,
        make_path: F,
    ) {
        let make_destination = |metadata: &ExecutionMetadata| (make_path)(metadata.global_id);

        self.add_operator(|prev| {
            let writer = AvroSink::new();
            WriterOperator::new(prev, writer, make_destination)
        })
        .finalize_block();
    }

    /// Write output to avro files. A avro is created for each replica of the current block.
    /// A file with a numerical suffix is created according to the path passed as parameter.
    ///
    /// + If the input is a directory numbered files will be created as output.
    /// + If the input is a file name the basename will be kept as prefix and numbers will
    ///   be added as suffix while keeping the same extension for the output.
    ///
    /// ## Example
    ///
    /// + `template_path`: `/data/renoir/output.avro` -> `/data/renoir/output0000.avro`, /data/renoir/output0001.avro` ...
    /// + `template_path`: `/data/renoir/` -> `/data/renoir/0000.avro`, /data/renoir/0001.avro` ...
    pub fn write_avro_seq(self, template_path: PathBuf) {
        self.add_operator(|prev| {
            let writer = AvroSink::new();
            WriterOperator::new(prev, writer, |m| sequential_path(template_path, m))
        })
        .finalize_block();
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: AvroSchema + ExchangeData,
{
    pub fn write_avro_one<P: Into<PathBuf>>(self, path: P) {
        let path = path.into();
        self.repartition(Replication::One, NextStrategy::only_one())
            .add_operator(|prev| {
                let writer = AvroSink::new();
                WriterOperator::new(prev, writer, move |_| path)
            })
            .finalize_block();
    }
}
