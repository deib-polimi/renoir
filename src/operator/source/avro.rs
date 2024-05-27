use std::fmt::Display;
use std::fs::File;
use std::io::BufReader;
use std::marker::PhantomData;
use std::path::PathBuf;

use apache_avro::Reader;
use serde::Deserialize;

use crate::block::{BlockStructure, OperatorKind, OperatorStructure, Replication};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

/// Source that reads and parses a CSV file.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
pub struct AvroSource<Out: Data + for<'a> Deserialize<'a>> {
    /// Path of the file.
    path: PathBuf,
    /// Reader used to parse the CSV file.
    reader: Option<Reader<'static, BufReader<File>>>,
    /// Whether the reader has terminated its job.
    terminated: bool,
    _out: PhantomData<Out>,
}

impl<Out: Data + for<'a> Deserialize<'a>> Display for AvroSource<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AvroSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> AvroSource<Out> {
    /// Create a new source that reads and parse the lines of a CSV file.
    ///
    /// The file is partitioned into as many chunks as replicas, each replica has to have the
    /// **same** file in the same path. It is guaranteed that each line of the file is emitted by
    /// exactly one replica.
    ///
    /// After creating the source it's possible to customize its behaviour using one of the
    /// available methods. By default it is assumed that the delimiter is `,` and the CSV has
    /// headers.
    ///
    /// Each line will be deserialized into the type `Out`, so the structure of the CSV must be
    /// valid for that deserialization. The [`csv`](https://crates.io/crates/csv) crate is used for
    /// the parsing.
    ///
    /// **Note**: the file must be readable and its size must be available. This means that only
    /// regular files can be read.
    ///
    /// ## Example
    ///
    /// ```
    /// # use renoir::{StreamContext, RuntimeConfig};
    /// # use renoir::operator::source::AvroSource;
    /// # use serde::{Deserialize, Serialize};
    /// # let mut env = StreamContext::new_local();
    /// #[derive(Clone, Deserialize, Serialize)]
    /// struct Thing {
    ///     what: String,
    ///     count: u64,
    /// }
    /// let source = AvroSource::<Thing>::new("/datasets/huge.csv");
    /// let s = env.stream(source);
    /// ```
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            reader: None,
            terminated: false,
            _out: PhantomData,
        }
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Source for AvroSource<Out> {
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Operator for AvroSource<Out> {
    type Out = Out;

    fn setup(&mut self, _metadata: &mut ExecutionMetadata) {
        // let global_id = metadata.global_id;
        // let instances = metadata.replicas.len();

        let file = File::options()
            .read(true)
            .write(false)
            .open(&self.path)
            .unwrap_or_else(|err| {
                panic!(
                    "AvroSource: error while opening file {:?}: {:?}",
                    self.path, err
                )
            });

        let buf_reader = BufReader::new(file);
        let reader = Reader::new(buf_reader).expect("failed to create avro reader");

        self.reader = Some(reader);
    }

    fn next(&mut self) -> StreamElement<Out> {
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
                StreamElement::Item(
                    apache_avro::from_value(&el)
                        .expect("could not deserialize from avro Value to specified type"),
                )
            }
            Some(Err(e)) => panic!("Error while reading Aveo file: {:?}", e),
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("AvroSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Clone for AvroSource<Out> {
    fn clone(&self) -> Self {
        assert!(
            self.reader.is_none(),
            "AvroSource must be cloned before calling setup"
        );
        Self {
            path: self.path.clone(),
            reader: None,
            terminated: false,
            _out: PhantomData,
        }
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `AvroSource` and makes a stream using `StreamContext::stream`
    pub fn stream_avro<T: Data + for<'a> Deserialize<'a>>(
        &self,
        path: impl Into<PathBuf>,
    ) -> Stream<AvroSource<T>> {
        let source = AvroSource::new(path);
        self.stream(source)
    }
}

#[cfg(test)]
mod tests {
    // use std::io::Write;

    // use itertools::Itertools;
    // use serde::{Deserialize, Serialize};
    // use tempfile::NamedTempFile;

    // use crate::config::RuntimeConfig;
    // use crate::environment::StreamContext;
    // use crate::operator::source::AvroSource;

    // #[test]
    // fn csv_without_headers() {
    //     for num_records in 0..100 {
    //         for terminator in &["\n", "\r\n"] {
    //             let file = NamedTempFile::new().unwrap();
    //             for i in 0..num_records {
    //                 write!(file.as_file(), "{},{}{}", i, i + 1, terminator).unwrap();
    //             }

    //             let env = StreamContext::new(RuntimeConfig::local(4).unwrap());
    //             let source = AvroSource::<(i32, i32)>::new(file.path()).has_headers(false);
    //             let res = env.stream(source).shuffle().collect_vec();
    //             env.execute_blocking();

    //             let mut res = res.get().unwrap();
    //             res.sort_unstable();
    //             assert_eq!(res, (0..num_records).map(|x| (x, x + 1)).collect_vec());
    //         }
    //     }
    // }

    // #[test]
    // fn csv_with_headers() {
    //     #[derive(Clone, Serialize, Deserialize)]
    //     struct T {
    //         a: i32,
    //         b: i32,
    //     }

    //     for num_records in 0..100 {
    //         for terminator in &["\n", "\r\n"] {
    //             let file = NamedTempFile::new().unwrap();
    //             write!(file.as_file(), "a,b{terminator}").unwrap();
    //             for i in 0..num_records {
    //                 write!(file.as_file(), "{},{}{}", i, i + 1, terminator).unwrap();
    //             }

    //             let env = StreamContext::new(RuntimeConfig::local(4).unwrap());
    //             let source = AvroSource::<T>::new(file.path());
    //             let res = env.stream(source).shuffle().collect_vec();
    //             env.execute_blocking();

    //             let res = res
    //                 .get()
    //                 .unwrap()
    //                 .into_iter()
    //                 .map(|x| (x.a, x.b))
    //                 .sorted()
    //                 .collect_vec();
    //             assert_eq!(res, (0..num_records).map(|x| (x, x + 1)).collect_vec());
    //         }
    //     }
    // }
}
