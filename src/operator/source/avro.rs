use std::fmt::Display;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

use apache_avro::types::Value;
use apache_avro::{from_value, Reader};
use serde::Deserialize;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::{CoordUInt, Stream};

pub trait MakeReader: Send + Clone {
    type Reader: Read + Send;
    fn make_reader(&self, index: CoordUInt, peers: CoordUInt) -> Self::Reader;
}

#[derive(Clone)]
pub struct MakeFileReader {
    path: PathBuf,
}

impl MakeReader for MakeFileReader {
    type Reader = BufReader<File>;
    fn make_reader(&self, _: CoordUInt, _: CoordUInt) -> Self::Reader {
        let file = File::options()
            .read(true)
            .write(false)
            .open(&self.path)
            .expect("could not open file");
        BufReader::new(file)
    }
}

impl<T, R> MakeReader for T
where
    T: Fn(CoordUInt, CoordUInt) -> R + Send + Clone,
    R: Read + Send,
{
    type Reader = R;
    fn make_reader(&self, index: CoordUInt, peers: CoordUInt) -> Self::Reader {
        (self)(index, peers)
    }
}

pub struct AvroSource<R: MakeReader> {
    replication: Replication,

    make_reader: R,
    reader: Option<Reader<'static, R::Reader>>,

    terminated: bool,
}

impl<R: MakeReader> Display for AvroSource<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AvroSource<{}>", std::any::type_name::<Value>())
    }
}

impl AvroSource<MakeFileReader> {
    pub fn from_file<P: Into<PathBuf> + Send + Clone>(replication: Replication, path: P) -> Self {
        Self {
            replication,
            make_reader: MakeFileReader { path: path.into() },
            reader: None,
            terminated: false,
        }
    }
}

impl<F: MakeReader> AvroSource<F> {
    pub fn from_fn(replication: Replication, f: F) -> Self {
        Self {
            replication,
            make_reader: f,
            reader: None,
            terminated: false,
        }
    }
}

impl<R: MakeReader> Source for AvroSource<R> {
    fn replication(&self) -> Replication {
        self.replication
    }
}

impl<R: MakeReader> Operator for AvroSource<R> {
    type Out = Value;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let global_id = metadata.global_id;
        let instances = metadata.replicas.len() as CoordUInt;

        self.reader = Some(
            Reader::new(self.make_reader.make_reader(global_id, instances))
                .expect("failed to create avro reader"),
        );
    }

    fn next(&mut self) -> StreamElement<Value> {
        if self.terminated {
            return StreamElement::Terminate;
        }
        let reader = self
            .reader
            .as_mut()
            .expect("AvroSource was not initialized");

        match reader.next() {
            Some(Ok(el)) => {
                tracing::trace!("avro Value: {el:?}");
                StreamElement::Item(el)
            }
            Some(Err(e)) => panic!("Error while reading Aveo file: {:?}", e),
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Value, _>("AvroSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<R: MakeReader + Clone> Clone for AvroSource<R> {
    fn clone(&self) -> Self {
        assert!(
            self.reader.is_none(),
            "AvroSource must be cloned before calling setup"
        );
        Self {
            reader: None,
            terminated: false,
            replication: self.replication,
            make_reader: self.make_reader.clone(),
        }
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `AvroSource` and makes a stream using `StreamContext::stream`
    pub fn stream_avro_file(
        &self,
        replication: Replication,
        path: impl Into<PathBuf>,
    ) -> Stream<AvroSource<MakeFileReader>> {
        let source = AvroSource::from_file(replication, path.into());
        self.stream(source)
    }

    pub fn stream_avro<
        F: Fn(CoordUInt, CoordUInt) -> R + Send + Clone + 'static,
        R: Read + Send,
    >(
        &self,
        replication: Replication,
        f: F,
    ) -> Stream<AvroSource<F>> {
        let source = AvroSource::from_fn(replication, f);
        self.stream(source)
    }
}

impl<Op> Stream<Op>
where
    Op: Operator<Out = Value> + 'static,
{
    pub fn from_avro_value<T: for<'de> Deserialize<'de> + Send>(
        self,
    ) -> Stream<impl Operator<Out = T>> {
        self.map(|v| {
            let de = from_value(&v);
            de.expect("failed to deserialize")
        })
    }
}
