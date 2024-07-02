use arrow::datatypes::Schema;
use arrow::json::reader::Decoder;
use arrow::json::ReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use crate::block::NextStrategy;
use crate::operator::sink::writer::WriteOperator;
use crate::operator::{ExchangeData, Operator};
use crate::{Replication, Stream};

use super::writer::{sequential_path, WriterOperator};

#[derive(Debug)]
pub struct ParquetSink<T> {
    // path: PathBuf,
    /// Reader used to parse the CSV file.
    writer: Option<ArrowWriter<BufWriter<File>>>,
    decoder: Option<Decoder>,
    schema: Arc<Schema>,
    _t: PhantomData<T>,
}

impl<T> Clone for ParquetSink<T> {
    fn clone(&self) -> Self {
        Self {
            _t: PhantomData,
            schema: self.schema.clone(),
            writer: None,
            decoder: None,
            // schema: self.schema.clone(),
        }
    }
}

impl<T> WriteOperator<T> for ParquetSink<T>
where
    T: Serialize + Send,
{
    type Destination = PathBuf;

    fn setup(&mut self, destination: Self::Destination) {
        let file = BufWriter::new(File::create(destination).unwrap());
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props)).unwrap();
        self.writer = Some(writer);
        self.decoder = Some(
            ReaderBuilder::new(self.schema.clone())
                .build_decoder()
                .unwrap(),
        );
    }

    fn write(&mut self, items: &mut impl Iterator<Item = T>) {
        self.decoder
            .as_mut()
            .unwrap()
            .serialize(&items.collect::<Vec<_>>())
            .expect("failed to serialize struct to arrow RecordBatch");
    }

    fn flush(&mut self) {
        if let Some(batch) = self
            .decoder
            .as_mut()
            .unwrap()
            .flush()
            .expect("failed to decode struct to arrow RecordBatch")
        {
            self.writer.as_mut().unwrap().write(&batch).unwrap();
            self.writer.as_mut().unwrap().flush().unwrap();
        }
    }

    fn finalize(&mut self) {
        self.writer.take().unwrap().close().unwrap();
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: Serialize,
{
    pub fn write_parquet_seq<P: Into<PathBuf>>(self, path: P, schema: Schema) {
        let writer = ParquetSink {
            writer: None,
            decoder: None,
            schema: Arc::new(schema),
            _t: PhantomData,
        };
        let path = path.into();
        self.add_operator(|prev| {
            WriterOperator::new(prev, writer, |meta| sequential_path(path, meta))
        })
        .finalize_block();
    }
}

impl<Op> Stream<Op>
where
    Op: Operator<Out: ExchangeData> + 'static,
{
    pub fn write_parquet_one<P: Into<PathBuf>>(self, path: P, schema: Schema) {
        let writer = ParquetSink {
            writer: None,
            decoder: None,
            schema: Arc::new(schema),
            _t: PhantomData,
        };
        let path = path.into();

        if matches!(self.block.scheduling.replication, Replication::One) {
            self.add_operator(|prev| WriterOperator::new(prev, writer, |_| path))
                .finalize_block();
        } else {
            self.repartition(Replication::One, NextStrategy::only_one())
                .add_operator(|prev| WriterOperator::new(prev, writer, |_| path))
                .finalize_block();
        }
    }
}
