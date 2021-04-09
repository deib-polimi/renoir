use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::mem::swap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::time::Instant;

use regex::Regex;

fn main() {
    let path = std::env::args().nth(1).unwrap();
    let path = PathBuf::from(&path);
    let n_threads = usize::from_str(&std::env::args().nth(2).unwrap()).unwrap();

    let (sink_sender, sink_receiver) = channel();

    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..n_threads {
        let (sender, receiver) = channel();
        senders.push(sender);
        receivers.push(receiver);
    }
    let start = Instant::now();
    // - source: read file, tokenize, flat_map, group_by
    for i in 0..n_threads {
        let senders = senders.clone();
        let path = path.clone();
        std::thread::Builder::new()
            .name(format!("Mapper{}", i))
            .spawn(move || {
                let global_id = i;
                let num_replicas = n_threads;

                let file = File::open(&path).expect("FileSource: error while opening file");
                let file_size = file.metadata().unwrap().len() as usize;
                println!("File is {}, size {}", path.display(), file_size,);

                let range_size = file_size / num_replicas;
                let start = range_size * global_id;
                let mut current = start;
                let end = if global_id == num_replicas - 1 {
                    file_size
                } else {
                    start + range_size
                };

                let mut reader = BufReader::new(file);
                // Seek reader to the first byte to be read
                reader
                    .seek(SeekFrom::Start(start as u64))
                    .expect("seek file");
                if global_id != 0 {
                    // discard first line
                    let mut s = String::new();
                    current += reader
                        .read_line(&mut s)
                        .expect("Cannot read line from file");
                }

                let tokenizer = Tokenizer::new();
                let mut batches = vec![];
                for _ in 0..n_threads {
                    batches.push(Vec::new());
                }
                let batch_size = 1000;

                while current <= end {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(len) if len > 0 => {
                            current += len;
                            let tokens = tokenizer.tokenize(line);
                            for token in tokens {
                                let mut hasher = DefaultHasher::new();
                                token.hash(&mut hasher);
                                let hash = hasher.finish() as usize;
                                batches[hash % senders.len()].push(token);
                            }
                        }
                        Ok(_) => break,
                        Err(e) => panic!("{:?}", e),
                    }
                    for i in 0..n_threads {
                        if batches[i].len() >= batch_size {
                            let mut batch = Vec::new();
                            swap(&mut batches[i], &mut batch);
                            senders[i].send(batch).unwrap();
                        }
                    }
                }
                for (i, batch) in batches.into_iter().enumerate() {
                    senders[i].send(batch).unwrap();
                }
                eprintln!("Mapper{} exited", i);
            })
            .unwrap();
    }

    drop(senders);

    // - reducer: recv, keyed reduce, to sink
    for (i, receiver) in receivers.into_iter().enumerate() {
        let sink_sender = sink_sender.clone();
        std::thread::Builder::new()
            .name(format!("Reducer{}", i))
            .spawn(move || {
                let mut hashmap: HashMap<_, usize> = HashMap::new();
                while let Ok(words) = receiver.recv() {
                    for word in words {
                        *hashmap.entry(word).or_default() += 1;
                    }
                }
                sink_sender.send(hashmap.into_iter().collect()).unwrap();
                eprintln!("Reducer{} exited", i);
            })
            .unwrap();
    }

    drop(sink_sender);

    // - collect: merge results
    let mut result = Vec::new();
    while let Ok(mut res) = sink_receiver.recv() {
        result.append(&mut res);
    }
    let duration = start.elapsed();
    println!("Elapsed: {:?}", duration);
    println!("Res: {}", result.len());
}

struct Tokenizer {
    re: Regex,
}

impl Tokenizer {
    fn new() -> Self {
        Self {
            re: Regex::new(r"[^A-Za-z]+").unwrap(),
        }
    }
    fn tokenize(&self, value: String) -> Vec<String> {
        self.re
            .replace_all(&value, " ")
            .split_ascii_whitespace()
            .filter(|word| !word.is_empty())
            .map(|t| t.to_lowercase())
            .collect()
    }
}
