use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fake::Fake;
use once_cell::sync::Lazy;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::operator::source::FileSource;
use noir::BatchMode;
use noir::StreamEnvironment;
use regex::Regex;

mod common;

static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[a-zA-Z]+").unwrap());

fn wordcount_fold(path: &Path) {
    let mut env = StreamEnvironment::default();

    let source = FileSource::new(path);
    let _result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by(|word| word.clone())
        .fold(0, |count, _word| *count += 1)
        .collect::<Vec<_>>();
    env.execute();
}

fn wordcount_fold_assoc(path: &Path) {
    let mut env = StreamEnvironment::default();

    let source = FileSource::new(path);
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by_fold(
            |w| w.clone(),
            0,
            |count, _word| *count += 1,
            |count1, count2| *count1 += count2,
        )
        .unkey();
    let _result = stream.collect_vec();
    env.execute();
}

fn wordcount_fold_assoc_kstring(path: &Path) {
    let mut env = StreamEnvironment::default();

    let source = FileSource::new(path);
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|m| m.as_str())
                .map(kstring::KString::from_ref)
                .collect::<Vec<_>>()
        })
        .group_by_fold(
            |w| w.clone(),
            0,
            |count, _word| *count += 1,
            |count1, count2| *count1 += count2,
        )
        .unkey();
    let _result = stream.collect_vec();
    env.execute();
}

fn wordcount_reduce(path: &Path) {
    let mut env = StreamEnvironment::default();

    let source = FileSource::new(path);
    let _result = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by(|word| word.clone())
        .map(|(_, word)| (word, 1))
        .reduce(|(_w1, c1), (_w2, c2)| *c1 += c2)
        .collect::<Vec<_>>();
    env.execute();
}

fn wordcount_reduce_assoc(path: &Path) {
    let mut env = StreamEnvironment::default();

    let source = FileSource::new(path);
    let stream = env
        .stream(source)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .map(|word| (word, 1))
        .group_by_reduce(|w| w.clone(), |(_w1, c1), (_w, c2)| *c1 += c2)
        .unkey();
    let _result = stream.collect_vec();
    env.execute();
}

fn wordcount_fast(path: &Path) {
    let mut env = StreamEnvironment::default();

    let stream = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<String, u64, BuildHasherDefault<wyhash::WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(8);
                for c in line.chars() {
                    if c.is_ascii_alphabetic() {
                        word.push(c.to_ascii_lowercase());
                    } else if !word.is_empty() {
                        let key = std::mem::replace(&mut word, String::with_capacity(8));
                        *acc.entry(key).or_default() += 1;
                    }
                }
            },
            |a, mut b| {
                for (k, v) in b.drain() {
                    *a.entry(k).or_default() += v;
                }
            },
        );
    let _result = stream.collect_vec();
    env.execute();
}

fn wordcount_fast_kstring(path: &Path) {
    let mut env = StreamEnvironment::default();

    let stream = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<kstring::KString, u64, BuildHasherDefault<wyhash::WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(8);
                for c in line.chars() {
                    if c.is_ascii_alphabetic() {
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
        );
    let _result = stream.collect_vec();
    env.execute();
}

fn wordcount_benchmark(c: &mut Criterion) {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let seed = b"rstream2 by edomora97 and mark03".to_owned();
    let r = &mut StdRng::from_seed(seed);

    const N_LINES: usize = 100_000;
    for _ in 0..N_LINES {
        use fake::faker::lorem::en::*;
        let line = Sentence(10..100).fake_with_rng::<String, _>(r);
        file.write_all(line.as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
    }

    let path = file.path();

    let mut group = c.benchmark_group("wordcount");
    group.sample_size(30);
    group.throughput(Throughput::Elements(N_LINES as u64));
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(12));
    group.bench_function("wordcount-fold", |b| {
        b.iter(|| wordcount_fold(black_box(path)))
    });
    group.bench_function("wordcount-fold-assoc", |b| {
        b.iter(|| wordcount_fold_assoc(black_box(path)))
    });
    group.bench_function("wordcount-fold-kstring", |b| {
        b.iter(|| wordcount_fold_assoc_kstring(black_box(path)))
    });
    group.bench_function("wordcount-reduce", |b| {
        b.iter(|| wordcount_reduce(black_box(path)))
    });
    group.bench_function("wordcount-reduce-assoc", |b| {
        b.iter(|| wordcount_reduce_assoc(black_box(path)))
    });
    group.bench_function("wordcount-fast", |b| {
        b.iter(|| wordcount_fast(black_box(path)))
    });
    group.bench_function("wordcount-fast-kstring", |b| {
        b.iter(|| wordcount_fast_kstring(black_box(path)))
    });
    group.finish();
}

criterion_group!(benches, wordcount_benchmark);
criterion_main!(benches);
