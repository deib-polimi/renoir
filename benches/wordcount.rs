use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::{BufWriter, Write};
use std::os::unix::prelude::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::{BenchmarkId, Throughput};
use fake::Fake;
use kstring::KString;
use once_cell::sync::Lazy;
use rand::prelude::StdRng;
use rand::SeedableRng;

use noir::BatchMode;
use noir::StreamEnvironment;

mod common;
use common::*;
use regex::Regex;
use wyhash::WyHash;

static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"[a-zA-Z]+").unwrap());

// #[global_allocator]
// static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

fn wordcount_bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("wordcount-line");
    g.sample_size(SAMPLES);
    g.warm_up_time(WARM_UP);
    g.measurement_time(DURATION);

    for lines in [0, 100, 10_000, 1_000_000] {
        let file = make_file(lines as usize);
        let file_size = file.as_file().metadata().unwrap().size();
        g.throughput(Throughput::Bytes(file_size));

        g.bench_with_input(
            BenchmarkId::new("wordcount-fold", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_fold(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-fold-assoc", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_fold_assoc(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-count-assoc", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_count_assoc(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-reduce", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_reduce(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-reduce-assoc", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_reduce_assoc(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-fast", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_fast(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-fast-kstring", lines),
            file.path(),
            |b, path| {
                b.iter(move || {
                    let mut env = StreamEnvironment::default();
                    wc_fast_kstring(&mut env, path);
                    env.execute_blocking();
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-fold-remote", lines),
            file.path(),
            |b, path| {
                let pathb = Arc::new(path.to_path_buf());
                b.iter(|| {
                    let p = pathb.clone();
                    remote_loopback_deploy(4, 4, move |env| wc_fold(env, &p));
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-fold-assoc-remote", lines),
            file.path(),
            |b, path| {
                let pathb = Arc::new(path.to_path_buf());
                b.iter(|| {
                    let p = pathb.clone();
                    remote_loopback_deploy(4, 4, move |env| wc_fold_assoc(env, &p));
                })
            },
        );

        g.bench_with_input(
            BenchmarkId::new("wordcount-fast-remote", lines),
            file.path(),
            |b, path| {
                let pathb = Arc::new(path.to_path_buf());
                b.iter(|| {
                    let p = pathb.clone();
                    remote_loopback_deploy(4, 4, move |env| wc_fast(env, &p));
                })
            },
        );
    }
    g.finish();
}

fn wc_fold(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            line.split_whitespace()
                .map(|s| s.to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by(|word: &String| word.clone())
        .fold(0u64, |count, _word| *count += 1)
        .collect_vec();
    std::hint::black_box(result);
}

fn wc_fold_assoc(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
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
        .unkey()
        .collect_vec();
    std::hint::black_box(result);
}

fn wc_count_assoc(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by_count(|w| w.clone())
        .unkey()
        .collect_vec();
    std::hint::black_box(result);
}

fn wc_reduce(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .group_by(|word| word.clone())
        .map(|(_, word)| (word, 1))
        .reduce(|(_w1, c1), (_w2, c2)| *c1 += c2)
        .collect_vec();
    std::hint::black_box(result);
}

fn wc_reduce_assoc(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .flat_map(move |line| {
            RE.find_iter(&line)
                .map(|s| s.as_str().to_lowercase())
                .collect::<Vec<_>>()
        })
        .map(|word| (word, 1))
        .group_by_reduce(|w| w.clone(), |(_w1, c1), (_w, c2)| *c1 += c2)
        .unkey()
        .collect_vec();
    std::hint::black_box(result);
}

fn wc_fast(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<KString, u64, BuildHasherDefault<WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(4);
                for c in line.chars() {
                    if !c.is_whitespace() {
                        word.push(c.to_ascii_lowercase());
                    } else if !word.is_empty() {
                        let key = KString::from_ref(word.as_str());
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
    std::hint::black_box(result);
}

fn wc_fast_kstring(env: &mut StreamEnvironment, path: &Path) {
    let result = env
        .stream_file(path)
        .batch_mode(BatchMode::fixed(1024))
        .fold_assoc(
            HashMap::<String, u64, BuildHasherDefault<WyHash>>::default(),
            |acc, line| {
                let mut word = String::with_capacity(4);
                for c in line.chars() {
                    if !c.is_whitespace() {
                        word.push(c.to_ascii_lowercase());
                    } else if !word.is_empty() {
                        let key = std::mem::replace(&mut word, String::with_capacity(4));
                        *acc.entry(key).or_default() += 1;
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
    std::hint::black_box(result);
}

criterion_group!(benches, wordcount_bench);
criterion_main!(benches);
