use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::block::NextStrategy;
use crate::block::Replication;
use crate::operator::{ExchangeData, Operator};
use crate::scheduler::ExecutionMetadata;
use crate::{CoordUInt, Stream};

use super::writer::{sequential_path, WriteOperator, WriterOperator};

// #[derive(Debug)]
pub struct CsvWriteOp<T> {
    _t: PhantomData<T>,
    append: bool,
    path: Option<PathBuf>,
    /// Reader used to parse the CSV file.
    writer: Option<csv::Writer<BufWriter<File>>>,
}

impl<T> CsvWriteOp<T>
where
    T: Serialize + Send,
{
    pub fn new(append: bool) -> Self {
        Self {
            _t: PhantomData,
            append,
            path: None,
            writer: None,
        }
    }
}

impl<T> WriteOperator<T> for CsvWriteOp<T>
where
    T: Serialize + Send,
{
    type Destination = PathBuf;

    fn setup(&mut self, destination: PathBuf) {
        self.path = Some(destination);

        tracing::debug!("Write csv to path {:?}", self.path.as_ref().unwrap());
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(!self.append)
            .append(self.append)
            .open(self.path.as_ref().unwrap())
            .unwrap_or_else(|err| {
                panic!(
                    "CsvSource: error while opening file {:?}: {:?}",
                    self.path, err
                )
            });
        let file_len = file.metadata().unwrap().len();

        let buf_writer = BufWriter::new(file);
        let csv_writer = csv::WriterBuilder::default()
            .has_headers(file_len == 0)
            .from_writer(buf_writer);

        self.writer = Some(csv_writer);
    }

    fn write(&mut self, items: &mut impl Iterator<Item = T>) {
        for item in items {
            self.writer.as_mut().unwrap().serialize(item).unwrap();
        }
    }

    fn flush(&mut self) {
        self.writer.as_mut().unwrap().flush().ok();
    }

    fn finalize(&mut self) {
        self.writer.take();
    }
}

impl<T> Clone for CsvWriteOp<T> {
    fn clone(&self) -> Self {
        Self {
            _t: PhantomData,
            append: self.append,
            path: None,
            writer: None,
        }
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: Serialize,
{
    pub fn write_csv<F: FnOnce(CoordUInt) -> PathBuf + Clone + Send + 'static>(
        self,
        make_path: F,
        append: bool,
    ) {
        let make_destination = |metadata: &ExecutionMetadata| (make_path)(metadata.global_id);

        self.add_operator(|prev| {
            let writer = CsvWriteOp::new(append);
            WriterOperator::new(prev, writer, make_destination)
        })
        .finalize_block();
    }

    /// Write output to CSV files. A CSV is created for each replica of the current block.
    /// A file with a numerical suffix is created according to the path passed as parameter.
    ///
    /// + If the input is a directory numbered files will be created as output.
    /// + If the input is a file name the basename will be kept as prefix and numbers will
    ///   be added as suffix while keeping the same extension for the output.
    ///
    /// ## Example
    ///
    /// + `template_path`: `/data/renoir/output.csv` -> `/data/renoir/output0000.csv`, /data/renoir/output0001.csv` ...
    /// + `template_path`: `/data/renoir/` -> `/data/renoir/0000.csv`, /data/renoir/0001.csv` ...
    pub fn write_csv_seq(self, template_path: PathBuf, append: bool) {
        self.add_operator(|prev| {
            let writer = CsvWriteOp::new(append);
            WriterOperator::new(prev, writer, |m| sequential_path(template_path, m))
        })
        .finalize_block();
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: ExchangeData,
{
    pub fn write_csv_one<P: Into<PathBuf>>(self, path: P, append: bool) {
        let path = path.into();
        self.repartition(Replication::One, NextStrategy::only_one())
            .add_operator(|prev| {
                let writer = CsvWriteOp::new(append);
                WriterOperator::new(prev, writer, move |_| path)
            })
            .finalize_block();
    }
}
