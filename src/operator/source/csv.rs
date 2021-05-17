use crate::block::{BlockStructure, OperatorKind, OperatorStructure};
use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use csv::{Reader, ReaderBuilder, Terminator, Trim};
use serde::Deserialize;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;

/// Wrapper that limits the bytes that can be read from a type that implements `io::Read`.
struct LimitedReader<R: Read> {
    inner: R,
    /// Bytes remaining to be read.
    remaining: usize,
}

impl<R: Read> LimitedReader<R> {
    fn new(inner: R, remaining: usize) -> Self {
        Self { inner, remaining }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_bytes = if self.remaining > 0 {
            // if there are some bytes to be read, call read on the inner reader
            self.inner.read(buf)?.min(self.remaining)
        } else {
            // all the bytes have been read
            0
        };
        self.remaining -= read_bytes;
        Ok(read_bytes)
    }
}

/// Options for the CSV parser.
#[derive(Clone)]
struct CsvOptions {
    /// Byte used to mark a line as a comment.
    comment: Option<u8>,
    /// Field delimiter.
    delimiter: u8,
    /// Whether quotes are escaped by using doubled quotes.
    double_quote: bool,
    /// Byte used to escape quotes.
    escape: Option<u8>,
    /// Whether to allow records with different number of fields.
    flexible: bool,
    /// Byte used to quote fields.
    quote: u8,
    /// Whether to enable field quoting.
    quoting: bool,
    /// Line terminator.
    terminator: Terminator,
    /// Whether to trim fields and/or headers.
    trim: Trim,
    /// Whether the CSV file has headers.
    has_headers: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            comment: None,
            delimiter: b',',
            double_quote: true,
            escape: None,
            flexible: false,
            quote: b'"',
            quoting: true,
            terminator: Terminator::CRLF,
            trim: Trim::None,
            has_headers: true,
        }
    }
}

/// Source that reads and parses a CSV file.
///
/// The file is divided in chunks and is read concurrently by multiple replicas.
pub struct CsvSource<Out: Data + for<'a> Deserialize<'a>> {
    /// Path of the file.
    path: PathBuf,
    /// Reader used to parse the CSV file.
    csv_reader: Option<Reader<LimitedReader<BufReader<File>>>>,
    /// Options to customize the CSV parser.
    options: CsvOptions,
    /// Whether the reader has terminated its job.
    terminated: bool,
    _out: PhantomData<Out>,
}

impl<Out: Data + for<'a> Deserialize<'a>> CsvSource<Out> {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            csv_reader: None,
            options: Default::default(),
            terminated: false,
            _out: PhantomData,
        }
    }

    pub fn comment(mut self, comment: Option<u8>) -> Self {
        self.options.comment = comment;
        self
    }

    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.options.delimiter = delimiter;
        self
    }

    pub fn double_quote(mut self, double_quote: bool) -> Self {
        self.options.double_quote = double_quote;
        self
    }

    pub fn escape(mut self, escape: Option<u8>) -> Self {
        self.options.escape = escape;
        self
    }

    pub fn flexible(mut self, flexible: bool) -> Self {
        self.options.flexible = flexible;
        self
    }

    pub fn quote(mut self, quote: u8) -> Self {
        self.options.quote = quote;
        self
    }

    pub fn quoting(mut self, quoting: bool) -> Self {
        self.options.quoting = quoting;
        self
    }

    pub fn terminator(mut self, terminator: Terminator) -> Self {
        self.options.terminator = terminator;
        self
    }

    pub fn trim(mut self, trim: Trim) -> Self {
        self.options.trim = trim;
        self
    }

    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.options.has_headers = has_headers;
        self
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Source<Out> for CsvSource<Out> {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Operator<Out> for CsvSource<Out> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let global_id = metadata.global_id;
        let num_replicas = metadata.replicas.len();

        let file = File::open(&self.path).unwrap_or_else(|err| {
            panic!(
                "CsvSource: error while opening file {:?}: {:?}",
                self.path, err
            )
        });

        let file_size = file.metadata().unwrap().len();

        let mut buf_reader = BufReader::new(file);

        let last_byte_terminator = match self.options.terminator {
            Terminator::CRLF => b'\n',
            Terminator::Any(terminator) => terminator,
            _ => unreachable!(),
        };

        // Handle the header
        let mut header = Vec::new();
        let header_size = if self.options.has_headers {
            buf_reader
                .read_until(last_byte_terminator, &mut header)
                .expect("Error while reading CSV header") as u64
        } else {
            0
        };

        // Calculate start and end offset of this replica
        let body_size = file_size - header_size;
        let range_size = body_size / num_replicas as u64;
        let mut start = header_size + range_size * global_id as u64;
        let mut end = if global_id == num_replicas - 1 {
            file_size
        } else {
            start + range_size
        };

        // Align start byte
        if global_id != 0 {
            // Seek reader to the first byte to be read
            buf_reader
                .seek(SeekFrom::Start(start))
                .expect("Error while seeking BufReader to start");
            // discard first line
            let mut buf = Vec::new();
            start += buf_reader
                .read_until(last_byte_terminator, &mut buf)
                .expect("Error while reading first line from file") as u64;
        }

        // Align end byte
        if global_id != num_replicas - 1 {
            // Seek reader to the last byte to be read
            buf_reader
                .seek(SeekFrom::Start(end))
                .expect("Error while seeking BufReader to end");
            // get to the end of the line
            let mut buf = Vec::new();
            end += buf_reader
                .read_until(last_byte_terminator, &mut buf)
                .expect("Error while reading last line from file") as u64;
        }

        // Rewind BufReader to the start
        buf_reader
            .seek(SeekFrom::Start(start))
            .expect("Error while rewinding BufReader");

        // Limit the number of bytes to be read
        let limited_reader = LimitedReader::new(buf_reader, (end - start) as usize);

        // Create csv::Reader
        let mut csv_reader = ReaderBuilder::new()
            .comment(self.options.comment)
            .delimiter(self.options.delimiter)
            .double_quote(self.options.double_quote)
            .escape(self.options.escape)
            .flexible(self.options.flexible)
            .quote(self.options.quote)
            .quoting(self.options.quoting)
            .terminator(self.options.terminator)
            .trim(self.options.trim)
            .has_headers(self.options.has_headers)
            .from_reader(limited_reader);

        if self.options.has_headers {
            // set the headers of the CSV file
            csv_reader.set_byte_headers(
                Reader::from_reader(header.as_slice())
                    .byte_headers()
                    .unwrap()
                    .to_owned(),
            );
        }

        self.csv_reader = Some(csv_reader);
    }

    fn next(&mut self) -> StreamElement<Out> {
        if self.terminated {
            return StreamElement::Terminate;
        }
        let csv_reader = self
            .csv_reader
            .as_mut()
            .expect("CsvSource was not initialized");

        match csv_reader.deserialize::<Out>().next() {
            Some(item) => StreamElement::Item(item.unwrap()),
            None => {
                self.terminated = true;
                StreamElement::FlushAndRestart
            }
        }
    }

    fn to_string(&self) -> String {
        format!("CsvSource<{}>", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("CSVSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl<Out: Data + for<'a> Deserialize<'a>> Clone for CsvSource<Out> {
    fn clone(&self) -> Self {
        assert!(
            self.csv_reader.is_none(),
            "CsvSource must be cloned before calling setup"
        );
        Self {
            path: self.path.clone(),
            csv_reader: None,
            options: self.options.clone(),
            terminated: false,
            _out: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::source::CsvSource;
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn csv_without_headers() {
        for num_records in 0..100 {
            for terminator in &["\n", "\r\n"] {
                let file = NamedTempFile::new().unwrap();
                for i in 0..num_records {
                    write!(file.as_file(), "{},{}{}", i, i + 1, terminator).unwrap();
                }

                let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
                let source = CsvSource::<(i32, i32)>::new(file.path()).has_headers(false);
                let res = env.stream(source).shuffle().collect_vec();
                env.execute();

                let mut res = res.get().unwrap();
                res.sort_unstable();
                assert_eq!(res, (0..num_records).map(|x| (x, x + 1)).collect_vec());
            }
        }
    }

    #[test]
    fn csv_with_headers() {
        #[derive(Clone, Serialize, Deserialize)]
        struct T {
            a: i32,
            b: i32,
        }

        for num_records in 0..100 {
            for terminator in &["\n", "\r\n"] {
                let file = NamedTempFile::new().unwrap();
                write!(file.as_file(), "a,b{}", terminator).unwrap();
                for i in 0..num_records {
                    write!(file.as_file(), "{},{}{}", i, i + 1, terminator).unwrap();
                }

                let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
                let source = CsvSource::<T>::new(file.path());
                let res = env.stream(source).shuffle().collect_vec();
                env.execute();

                let res = res
                    .get()
                    .unwrap()
                    .into_iter()
                    .map(|x| (x.a, x.b))
                    .sorted()
                    .collect_vec();
                assert_eq!(res, (0..num_records).map(|x| (x, x + 1)).collect_vec());
            }
        }
    }
}
