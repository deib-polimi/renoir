use std::fmt::Display;
use std::fs::File;
use std::io::BufRead;
use std::io::Seek;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;

use crate::block::Replication;
use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::network::Coord;
use crate::operator::source::Source;
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::Stream;

/// Source that reads a text file line-by-line.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
#[derive(Debug)]
pub struct FileSource {
    path: PathBuf,
    // reader is initialized in `setup`, before it is None
    reader: Option<BufReader<File>>,
    current: usize,
    end: usize,
    terminated: bool,
    coord: Option<Coord>,
}

impl Display for FileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FileSource<{}>", std::any::type_name::<String>())
    }
}

impl FileSource {
    /// Create a new source that reads the lines from a text file.
    ///
    /// The file is partitioned into as many chunks as replicas, each replica has to have the
    /// **same** file in the same path. It is guaranteed that each line of the file is emitted by
    /// exactly one replica.
    ///
    /// **Note**: the file must be readable and its size must be available. This means that only
    /// regular files can be read.
    ///
    /// ## Example
    ///
    /// ```
    /// # use noir::{StreamEnvironment, EnvironmentConfig};
    /// # use noir::operator::source::FileSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let source = FileSource::new("/datasets/huge.txt");
    /// let s = env.stream(source);
    /// ```
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            reader: Default::default(),
            current: 0,
            end: 0,
            terminated: false,
            coord: None,
        }
    }
}

impl Source<String> for FileSource {
    fn replication(&self) -> Replication {
        Replication::Unlimited
    }
}

impl Operator<String> for FileSource {
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        let global_id = metadata.global_id;
        let instances = metadata.replicas.len();

        let file = File::open(&self.path).unwrap_or_else(|err| {
            panic!(
                "FileSource: error while opening file {:?}: {:?}",
                self.path, err
            )
        });
        let file_size = file.metadata().unwrap().len() as usize;

        let range_size = file_size / instances;
        let start = range_size * global_id as usize;
        self.current = start;
        self.end = if global_id as usize == instances - 1 {
            file_size
        } else {
            start + range_size
        };

        let mut reader = BufReader::new(file);
        // Seek reader to the first byte to be read
        reader
            .seek(SeekFrom::Current(start as i64))
            .expect("seek file");
        if global_id != 0 {
            // discard first line
            let mut v = Vec::new();

            self.current += reader
                .read_until(b'\n', &mut v)
                .expect("Cannot read line from file");
        }
        self.coord = Some(metadata.coord);
        self.reader = Some(reader);
    }

    fn next(&mut self) -> StreamElement<String> {
        if self.terminated {
            log::trace!("terminate {}", self.coord.unwrap());
            return StreamElement::Terminate;
        }
        let element = if self.current <= self.end {
            let mut line = String::new();
            match self
                .reader
                .as_mut()
                .expect("BufReader was not initialized")
                .read_line(&mut line)
            {
                Ok(len) if len > 0 => {
                    self.current += len;
                    StreamElement::Item(line)
                }
                Ok(_) => {
                    self.terminated = true;
                    StreamElement::FlushAndRestart
                }
                Err(e) => panic!("Error while reading file: {e:?}",),
            }
        } else {
            self.terminated = true;
            StreamElement::FlushAndRestart
        };

        element
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<String, _>("FileSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl Clone for FileSource {
    fn clone(&self) -> Self {
        assert!(
            self.reader.is_none(),
            "FileSource must be cloned before calling setup"
        );
        FileSource {
            path: self.path.clone(),
            reader: None,
            current: 0,
            end: 0,
            terminated: false,
            coord: None,
        }
    }
}

impl crate::StreamEnvironment {
    /// Convenience method, creates a `FileSource` and makes a stream using `StreamEnvironment::stream`
    pub fn stream_file<P: Into<PathBuf>>(&mut self, path: P) -> Stream<String, FileSource> {
        let source = FileSource::new(path);
        self.stream(source)
    }
}
