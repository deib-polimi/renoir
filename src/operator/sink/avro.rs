use apache_avro::{AvroSchema, Schema, Writer};
use serde::Serialize;
use std::fmt::Display;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

// #[derive(Debug)]
pub struct AvroSink<Op>
where
    Op: Operator,
{
    prev: Op,
    path: PathBuf,
    /// Reader used to parse the CSV file.
    writer: Option<BufWriter<File>>,
    schema: Schema,
}

impl<Op> AvroSink<Op>
where
    Op: Operator,
    Op::Out: AvroSchema,
{
    pub fn new<P: Into<PathBuf>>(prev: Op, path: P) -> Self {
        Self {
            path: path.into(),
            prev,
            writer: None,
            schema: Op::Out::get_schema(),
        }
    }
}

impl<Op> Display for AvroSink<Op>
where
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> AvroSink<{}>",
            self.prev,
            std::any::type_name::<Op::Out>()
        )
    }
}

impl<Op> Operator for AvroSink<Op>
where
    Op: Operator,
    Op::Out: AvroSchema + Serialize,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)
            .unwrap_or_else(|err| {
                panic!(
                    "AvroSource: error while opening file {:?}: {:?}",
                    self.path, err
                )
            });

        let buf_writer = BufWriter::new(file);
        self.writer = Some(buf_writer);
    }

    fn next(&mut self) -> StreamElement<()> {
        let writer = self.writer.as_mut().unwrap();
        let mut w = Writer::new(&self.schema, writer);
        loop {
            match self.prev.next() {
                StreamElement::Item(t) | StreamElement::Timestamped(t, _) => {
                    // w.extend_ser(values)
                    w.append_ser(t).expect("failed to write to avro");
                }
                el => {
                    w.flush().unwrap();
                    return el.map(|_| ());
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>("AvroSink");
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

impl<Op> Clone for AvroSink<Op>
where
    Op: Operator,
{
    fn clone(&self) -> Self {
        panic!("AvroSink cannot be cloned, replication should be 1");
    }
}

impl<Op: Operator> Stream<Op>
where
    Op: 'static,
    Op::Out: AvroSchema + Serialize,
{
    /// Apply the given function to all the elements of the stream, consuming the stream.
    ///
    /// ## Example
    ///
    /// ```
    /// # use renoir::{StreamContext, RuntimeConfig};
    /// # use renoir::operator::source::IteratorSource;
    /// # let mut env = StreamContext::new_local();
    /// let s = env.stream_iter(0..5).group_by(|&n| n % 2);
    /// s.for_each(|(key, n)| println!("Item: {} has key {}", n, key));
    ///
    /// env.execute_blocking();
    /// ```
    pub fn write_avro<P: Into<PathBuf>>(self, path: P) {
        self.add_operator(|prev| AvroSink::new(prev, path))
            .finalize_block();
    }
}
