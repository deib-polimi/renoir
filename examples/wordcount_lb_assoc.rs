use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::mpsc::channel;
use std::time::Instant;

use itertools::Itertools;
use regex::Regex;

fn main() {
    let path = std::env::args().nth(1).unwrap();
    let path = PathBuf::from(&path);
    let n_threads = usize::from_str(&std::env::args().nth(2).unwrap()).unwrap();

    let (sink_sender, sink_receiver) = channel();

    let start = Instant::now();
    // - source: read file, tokenize, flat_map, reduce
    for i in 0..n_threads {
        let sink_sender = sink_sender.clone();
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
                let mut hashmap: HashMap<_, u32> = HashMap::default();

                while current <= end {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(len) if len > 0 => {
                            current += len;
                            let tokens = tokenizer.tokenize(line);
                            for token in tokens {
                                *hashmap.entry(token).or_default() += 1;
                            }
                        }
                        Ok(_) => break,
                        Err(e) => panic!("{:?}", e),
                    }
                }
                sink_sender.send(hashmap.into_iter().collect_vec()).unwrap();
                eprintln!("Mapper{} exited", i);
            })
            .unwrap();
    }

    drop(sink_sender);

    // - collect: merge results
    let mut result: HashMap<_, u32> = HashMap::default();
    while let Ok(res) = sink_receiver.recv() {
        for (word, count) in res {
            *result.entry(word).or_default() += count;
        }
    }
    let duration = start.elapsed();
    println!("Elapsed: {:?}", duration);
    println!("Res: {}", result.len());
    let mut check = 1usize;
    for count in result.values() {
        check = check.wrapping_mul(*count as usize) % 1000000007usize;
    }
    println!("Check = {}", check);
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
