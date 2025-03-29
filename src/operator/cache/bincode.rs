use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Write};
use std::path::PathBuf;

use bincode::config;
use bincode::error::DecodeError;
use serde::{Deserialize, Serialize};

use super::{CacheReplayer, Cacher};

pub struct BincodeCacheConfig {
    pub batch_size: usize,
    pub path: PathBuf,
}

pub struct BincodeCacher<T> {
    buf: Vec<T>,
    batch_size: usize,
    path: PathBuf,
    file: BufWriter<File>,
}

pub struct BincodeReplayer<T> {
    buf: std::vec::IntoIter<T>,
    // path: PathBuf,
    file: BufReader<File>,
}

impl<T: Serialize> BincodeCacher<T> {
    fn flush_buf(&mut self) {
        // let len = BINCODE_CONFIG.serialized_size(&self.buf).unwrap();
        // self.file.write_all(&len.to_le_bytes()).unwrap();
        bincode::serde::encode_into_std_write(&self.buf, &mut self.file, config::standard())
            .unwrap();

        self.buf.truncate(0);
    }
}

impl<T: Serialize + for<'de> Deserialize<'de> + Send + 'static> Cacher<T> for BincodeCacher<T> {
    type Config = BincodeCacheConfig;
    type Handle = PathBuf;
    type Replayer = BincodeReplayer<T>;

    fn init(config: &BincodeCacheConfig, coord: crate::network::Coord) -> Self {
        let mut path = config.path.clone();
        path.push(format!(
            "{:02}.{:02}.{:04}.rnbc",
            coord.host_id, coord.block_id, coord.replica_id
        ));
        let file = BufWriter::new(File::create(&path).unwrap());
        Self {
            buf: Vec::with_capacity(config.batch_size),
            batch_size: config.batch_size,
            path,
            file,
        }
    }

    fn append(&mut self, item: T) {
        self.buf.push(item);
        if self.buf.len() == self.batch_size {
            self.flush_buf();
        }
    }

    fn finalize(mut self) -> Self::Handle {
        if !self.buf.is_empty() {
            self.flush_buf();
        }
        self.file.flush().unwrap();
        self.path
    }
}

impl<T: for<'de> Deserialize<'de> + Send> CacheReplayer<T> for BincodeReplayer<T> {
    type Handle = PathBuf;

    fn new(handle: Self::Handle) -> Self {
        let file = BufReader::new(File::open(&handle).unwrap());

        Self {
            buf: Default::default(),
            // path: handle,
            file,
        }
    }

    fn next(&mut self) -> Option<T> {
        if let Some(el) = self.buf.next() {
            return Some(el);
        }

        match bincode::serde::decode_from_std_read::<Vec<T>, _, _>(
            &mut self.file,
            config::standard(),
        ) {
            Ok(data) => self.buf = data.into_iter(),
            Err(DecodeError::Io { inner, .. }) if inner.kind() == ErrorKind::UnexpectedEof => {
                return None
            }
            Err(e) => {
                panic!("{e}");
            }
        }
        self.next()
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use crate::{
        operator::source::{IteratorSource, ParallelIteratorSource},
        StreamContext,
    };

    use super::{BincodeCacheConfig, BincodeCacher};

    fn cache_config() -> (BincodeCacheConfig, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = BincodeCacheConfig {
            batch_size: 64,
            path: temp_dir.path().to_owned(),
        };
        (config, temp_dir)
    }

    #[test]
    #[should_panic(
        expected = "Reading cache before it was complete. execution from a cached stream must start after the previous StreamContext has completed!"
    )]
    fn read_before_execution() {
        let env = StreamContext::new_local();
        let source = ParallelIteratorSource::new(0..10);

        let (config, _dir) = cache_config();

        let (cache, _s) = env
            .stream(source)
            .map(|x| x + 1)
            .cache::<BincodeCacher<_>>(config);

        cache.inner_cloned();
    }

    #[test]
    fn one_to_one() {
        tracing_subscriber::fmt::SubscriberBuilder::default()
            .with_max_level(tracing::Level::DEBUG)
            .init();
        let (config, _dir) = cache_config();
        let ctx = StreamContext::new_local();
        let n = 1280;
        let source = IteratorSource::new(0..n);

        let cache = ctx
            .stream(source)
            .map(|x| x + 1)
            .collect_cache::<BincodeCacher<_>>(config);

        ctx.execute_blocking();

        // Restart from cache

        let ctx = StreamContext::new_local();
        let result = cache.stream_in(&ctx).map(|x| x * 3).collect_vec();

        ctx.execute_blocking();
        assert_eq!(
            result.get().unwrap(),
            (0..n).map(|x| (x + 1) * 3).collect::<Vec<_>>()
        );
    }

    #[test]
    fn many_to_many() {
        // tracing_subscriber::fmt::SubscriberBuilder::default()
        //     .with_max_level(tracing::Level::DEBUG)
        //     .init();
        let (config, _dir) = cache_config();
        let ctx = StreamContext::new_local();
        let n = 1280;
        let cache = ctx
            .stream_par_iter(0..n)
            .map(|x| x + 1)
            .collect_cache::<BincodeCacher<_>>(config);

        ctx.execute_blocking();

        let handles = cache.inner_cloned();
        eprintln!("handles: {handles:?}");

        // Pass through cache
        let (config, _dir) = cache_config();
        let ctx = StreamContext::new(cache.config());
        let cache2 = cache
            .stream_in(&ctx)
            .collect_cache::<BincodeCacher<_>>(config);
        ctx.execute_blocking();

        // Restart from cache

        let ctx = StreamContext::new(cache2.config());
        let result = cache2
            .stream_in(&ctx)
            .shuffle()
            .map(|x| x * 3)
            .collect_vec();

        ctx.execute_blocking();
        let mut result = result.get().unwrap();
        result.sort_unstable();
        assert_eq!(result, (0..n).map(|x| (x + 1) * 3).collect::<Vec<_>>());
    }
}
