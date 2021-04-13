use crate::operator::source::Source;
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;

/// Limits the bytes to be read from a type that implements `io::Read`
struct LimitedReader<R: Read> {
    inner: R,
    remaining: usize,
}

impl<R: Read> LimitedReader<R> {
    fn new(inner: R, remaining: usize) -> Self {
        Self { inner, remaining }
    }
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read_bytes = self.inner.read(buf)?.min(self.remaining);
        self.remaining -= read_bytes;
        Ok(read_bytes)
    }
}

pub struct CsvSource<Out: Data> {
    path: PathBuf,
    csv_reader: Option<Reader<LimitedReader<BufReader<File>>>>,
    has_headers: bool,
    _out: PhantomData<Out>,
}

impl<Out: Data> CsvSource<Out> {
    pub fn new<P: Into<PathBuf>>(path: P, has_headers: bool) -> Self {
        Self {
            path: path.into(),
            csv_reader: None,
            has_headers,
            _out: PhantomData,
        }
    }
}

impl<Out: Data> Source<Out> for CsvSource<Out> {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

impl<Out: Data> Operator<Out> for CsvSource<Out> {
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

        // Handle the header
        let mut header = Vec::new();
        let header_size = if self.has_headers {
            buf_reader
                .read_until(b'\n', &mut header)
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
                .read_until(b'\n', &mut buf)
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
                .read_until(b'\n', &mut buf)
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
            .has_headers(self.has_headers)
            .from_reader(limited_reader);

        if self.has_headers {
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
        let csv_reader = self
            .csv_reader
            .as_mut()
            .expect("CsvSource was not initialized");

        match csv_reader.deserialize::<Out>().next() {
            None => StreamElement::End,
            Some(item) => StreamElement::Item(item.unwrap()),
        }
    }

    fn to_string(&self) -> String {
        format!("CsvSource<{}>", std::any::type_name::<Out>())
    }
}

impl<Out: Data> Clone for CsvSource<Out> {
    fn clone(&self) -> Self {
        assert!(
            self.csv_reader.is_none(),
            "CsvSource must be cloned before calling setup"
        );
        Self {
            path: self.path.clone(),
            csv_reader: None,
            has_headers: self.has_headers,
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
                let source = CsvSource::<(i32, i32)>::new(file.path(), false);
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
                let source = CsvSource::<T>::new(file.path(), true);
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
