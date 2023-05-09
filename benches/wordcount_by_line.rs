use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::{BufWriter, Write};
use std::time::Duration;

use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion};
use fake::Fake;
use itertools::Itertools;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::operator::source::FileSource;
use noir::BatchMode;
use noir::StreamEnvironment;

mod common;
use common::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn make_file(lines: usize) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let seed = b"By imDema, edomora97 and mark03.".to_owned();
    let r = &mut StdRng::from_seed(seed);
    let mut w = BufWriter::new(&mut file);

    for _ in 0..lines {
        use fake::faker::lorem::en::*;
        let line = Sentence(10..100).fake_with_rng::<String, _>(r);
        w.write_all(line.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
    }
    drop(w);

    file
}

fn wordcount_by_line_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wordcount-line");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Elements(1));

    group.bench_function("wordcount-fold", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSource::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .flat_map(move |line| {
                        line.split_whitespace()
                            .map(|s| s.to_lowercase())
                            .collect::<Vec<_>>()
                    })
                    .group_by(|word: &String| word.clone())
                    .fold(0u64, |count, _word| *count += 1)
                    .collect_vec();
                (result, file)
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });

    group.bench_function("wordcount-fold-assoc", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSource::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .flat_map(move |line| {
                        line.split_whitespace()
                            .map(|s| s.to_lowercase())
                            .collect_vec()
                    })
                    .group_by_fold(
                        |w| w.clone(),
                        0,
                        |count, _word| *count += 1,
                        |count1, count2| *count1 += count2,
                    )
                    .unkey()
                    .collect_vec();
                (result, file)
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });

    group.bench_function("wordcount-count-assoc", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSource::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .flat_map(move |line| {
                        line.split_whitespace()
                            .map(|s| s.to_lowercase())
                            .collect_vec()
                    })
                    .group_by_count(|w| w.clone())
                    .unkey()
                    .collect_vec();
                (result, file)
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });

    group.bench_function("wordcount-reduce-assoc", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSource::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .flat_map(move |line| {
                        line.split_whitespace()
                            .map(|s| s.to_lowercase())
                            .collect_vec()
                    })
                    .map(|word| (word, 1))
                    .group_by_reduce(|w| w.clone(), |(_w1, c1), (_w, c2)| *c1 += c2)
                    .unkey()
                    .collect_vec();
                (result, file)
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });

    group.bench_function("wordcount-fast", |b| {
        let builder = NoirBenchBuilder::new(
            noir_max_parallism_env,
            |n: u64, env: &mut StreamEnvironment| {
                let file = make_file(n as usize);
                let path = file.path();
                let source = FileSource::new(path);
                let result = env
                    .stream(source)
                    .batch_mode(BatchMode::fixed(1024))
                    .fold_assoc(
                        HashMap::<kstring::KString, u64, BuildHasherDefault<wyhash::WyHash>>::default(),
                        |acc, line| {
                            let mut word = String::with_capacity(8);
                            for c in line.chars() {
                                if !c.is_whitespace() {
                                    word.push(c.to_ascii_lowercase());
                                } else if !word.is_empty() {
                                    let key = kstring::KString::from_ref(word.as_str());
                                    *acc.entry(key).or_default() += 1;
                                    word.truncate(0);
                                }
                            }
                        },
                        |a, mut b| {
                            for (k, v) in b.drain() {
                                *a.entry(k).or_default() += v;
                            }
                        },
                    )
                    .collect_vec();
                (result, file)
            },
        );
        b.iter_custom(|n| builder.bench(n))
    });
    group.finish();
}

criterion_group!(benches, wordcount_by_line_benchmark);
criterion_main!(benches);
