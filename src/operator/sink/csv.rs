use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::block::NextStrategy;
use crate::operator::{ExchangeData, Operator};
use crate::scheduler::ExecutionMetadata;
use crate::{CoordUInt, Replication, Stream};

use super::writer::{WriteOperator, WriterOperator};

// #[derive(Debug)]
pub struct CsvWriteOp<T, N> {
    _t: PhantomData<T>,
    make_path: Option<N>,
    append: bool,
    path: Option<PathBuf>,
    /// Reader used to parse the CSV file.
    writer: Option<csv::Writer<BufWriter<File>>>,
}

impl<T, N> CsvWriteOp<T, N>
where
    T: Serialize + Send,
    N: FnOnce(CoordUInt) -> PathBuf + Clone + Send + 'static,
{
    pub fn new(make_path: N, append: bool) -> Self {
        Self {
            _t: PhantomData,
            make_path: Some(make_path),
            append,
            path: None,
            writer: None,
        }
    }
}

impl<T, N> WriteOperator<T> for CsvWriteOp<T, N>
where
    T: Serialize + Send,
    N: FnOnce(CoordUInt) -> PathBuf + Clone + Send + 'static,
{
    fn setup(&mut self, metadata: &ExecutionMetadata) {
        let id = metadata.global_id;
        self.path = Some(self.make_path.take().unwrap()(id));

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

    fn write(&mut self, item: T) {
        self.writer.as_mut().unwrap().serialize(item).unwrap();
    }

    fn flush(&mut self) {
        self.writer.as_mut().unwrap().flush().ok();
    }

    fn finalize(&mut self) {
        self.writer.take();
    }
}

impl<T, N> Clone for CsvWriteOp<T, N>
where
    N: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _t: PhantomData,
            make_path: self.make_path.clone(),
            append: self.append,
            path: None,
            writer: None,
        }
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: Serialize + ExchangeData,
{
    pub fn write_csv<F: FnOnce(CoordUInt) -> PathBuf + Clone + Send + 'static>(
        self,
        make_path: F,
        append: bool,
    ) {
        self.add_operator(|prev| {
            let writer = CsvWriteOp::new(make_path, append);
            WriterOperator { prev, writer }
        })
        .finalize_block();
    }

    pub fn write_csv_one<P: Into<PathBuf>>(self, path: P, append: bool) {
        let path = path.into();
        self.repartition(Replication::One, NextStrategy::only_one())
            .add_operator(|prev| {
                let writer = CsvWriteOp::new(move |_| path, append);
                WriterOperator { prev, writer }
            })
            .finalize_block();
    }
}
