use async_std::stream;
use async_std::stream::StreamExt;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::operator::source::{Source, StreamSource};
use crate::operator::{Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use async_std::fs::File;
use async_std::io::{BufReader, SeekFrom};
use async_std::path::{Path, PathBuf};
use async_std::prelude::*;

#[derive(Debug)]
pub struct FileSource {
    path: PathBuf,
    // reader is initialized in `setup`, before it is None
    reader: Option<BufReader<File>>,
    current: usize,
    end: usize,
}

impl FileSource {
    pub fn new<P>(path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            path: path.into(),
            reader: Default::default(),
            current: 0,
            end: 0,
        }
    }
}

impl Source<String> for FileSource {
    fn get_max_parallelism(&self) -> Option<usize> {
        None
    }
}

#[async_trait]
impl Operator<String> for FileSource {
    async fn setup(&mut self, metadata: ExecutionMetadata) {
        let global_id = metadata.global_id;
        let num_replicas = metadata.num_replicas;

        let file = File::open(&self.path)
            .await
            .expect("FileSource: error while opening file");
        let file_size = file.metadata().await.unwrap().len() as usize;

        let range_size = file_size / num_replicas;
        let start = range_size * global_id;
        self.current = start;
        self.end = if global_id == num_replicas - 1 {
            file_size
        } else {
            start + range_size
        };

        let mut reader = BufReader::new(file);
        // Seek reader to the first byte to be read
        reader
            .seek(SeekFrom::Current(start as i64))
            .await
            .expect("seek file");
        if global_id != 0 {
            // discard first line
            let mut s = String::new();
            self.current += reader
                .read_line(&mut s)
                .await
                .expect("Cannot read line from file");
        }
        self.reader = Some(reader);
    }

    async fn next(&mut self) -> StreamElement<String> {
        let element = if self.current <= self.end {
            let mut line = String::new();
            match self
                .reader
                .as_mut()
                .expect("BufReader was not initialized")
                .read_line(&mut line)
                .await
            {
                Ok(len) if len > 0 => {
                    self.current += len;
                    StreamElement::Item(line)
                }
                Ok(_) => StreamElement::End,
                Err(e) => panic!(e),
            }
        } else {
            StreamElement::End
        };

        element
    }

    fn to_string(&self) -> String {
        format!("FileSource<{}>", std::any::type_name::<String>())
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
        }
    }
}
