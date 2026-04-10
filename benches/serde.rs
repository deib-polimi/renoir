use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};

use renoir::operator::StreamElement;
use renoir::CoordUInt;
use serde::{Deserialize, Serialize};

use bincode::config::{Configuration, Fixint, Limit, LittleEndian};

use std::hint::black_box;
pub(crate) const HEADER_SIZE: usize = 20; // std::mem::size_of::<MessageHeader>();
pub(crate) static BINCODE_HEADER: Configuration<LittleEndian, Fixint, Limit<HEADER_SIZE>> =
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_limit::<HEADER_SIZE>();

pub(crate) static BINCODE_MESSAGE: Configuration = bincode::config::standard();
/// Header of a message sent before the actual message.
#[derive(Serialize, Deserialize, Default)]
pub(crate) struct MessageHeader {
    /// The size of the actual message
    pub(crate) size: u32,
    /// The id of the replica this message is for.
    pub(crate) replica_id: CoordUInt,
    /// The id of the block that is sending the message.
    pub(crate) sender_block_id: CoordUInt,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
struct Coord {
    a: CoordUInt,
    b: CoordUInt,
    c: CoordUInt,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub enum NetworkData<T> {
    Batch(Vec<T>),
}

/// What is sent from a replica to the next.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize)]
pub struct NetworkMessage<T> {
    /// The coordinates of the block that sent this message.
    sender: Coord,
    /// The list of messages inside the batch,
    data: NetworkData<StreamElement<T>>,
}

fn bincode_serialize<T: Serialize>(scratch: &mut Vec<u8>, msg: &NetworkMessage<T>) {
    scratch.resize(HEADER_SIZE, 0);

    bincode::serde::encode_into_std_write(&msg, scratch, BINCODE_MESSAGE)
        .expect("failed to serialize");
    let serialized_len = scratch.len() - HEADER_SIZE;

    let header = MessageHeader {
        size: serialized_len.try_into().unwrap(),
        replica_id: 257,
        sender_block_id: 17,
    };

    bincode::serde::encode_into_slice(header, &mut scratch[0..HEADER_SIZE], BINCODE_HEADER)
        .expect("failed to serialize");
}

fn bincode_deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> NetworkMessage<T> {
    let (msg, _msg_len): (NetworkMessage<T>, _) =
        bincode::serde::decode_from_slice(data, BINCODE_MESSAGE).expect("Malformed message");
    msg
}

fn random_string(rng: &mut impl Rng, len: std::ops::Range<usize>) -> String {
    let b = rng.random_range(0..100);
    let len = rng.random_range(len);

    let mut s = String::with_capacity(len);
    for _ in 0..len {
        match b {
            0..50 => {
                s.push(rng.random_range('a'..='z'));
            }
            50..75 => {
                s.push(rng.random_range('A'..='Z'));
            }
            _ => {
                s.push(rng.random_range('0'..='1'));
            }
        }
    }
    s
}

#[derive(Clone, Serialize, Deserialize)]
struct SampleStringy {
    string_1: String,
    string_2: String,
    string_3: String,
    string_4: String,
    string_5: String,
    string_6: String,
    string_7: String,
    string_8: String,
    string_9: String,
    opt_string_10: Option<String>,
    string_11: String,
    string_12: String,
    string_13: String,
    opt_string_14: Option<String>,
    string_15: String,
    string_16: String,
    string_17: String,
    opt_string_18: Option<String>,
    string_19: String,
    string_20: String,
    string_21: String,
}

impl SampleStringy {
    pub fn random(rng: &mut impl Rng) -> Self {
        Self {
            string_1: random_string(rng, 4..16),
            string_2: random_string(rng, 4..16),
            string_3: random_string(rng, 4..16),
            string_4: random_string(rng, 4..16),
            string_5: random_string(rng, 4..16),
            string_6: random_string(rng, 4..16),
            string_7: random_string(rng, 4..64),
            string_8: random_string(rng, 4..16),
            string_9: random_string(rng, 4..8),
            opt_string_10: if rng.random_bool(0.5) {
                Some(random_string(rng, 4..16))
            } else {
                None
            },
            string_11: random_string(rng, 4..16),
            string_12: random_string(rng, 4..16),
            string_13: random_string(rng, 4..16),
            opt_string_14: if rng.random_bool(0.5) {
                Some(random_string(rng, 4..16))
            } else {
                None
            },
            string_15: random_string(rng, 4..16),
            string_16: random_string(rng, 4..16),
            string_17: random_string(rng, 4..16),
            opt_string_18: if rng.random_bool(0.5) {
                Some(random_string(rng, 4..16))
            } else {
                None
            },
            string_19: random_string(rng, 4..16),
            string_20: random_string(rng, 4..16),
            string_21: random_string(rng, 4..16),
        }
    }
}

fn serde_benchmark(c: &mut Criterion) {
    let seed = b"rstream2 by edomora97 and mark03".to_owned();

    let mut group = c.benchmark_group("serialization");
    for batch_size in [1, 16, 256, 1024, 4096, 8192] {
        let r = &mut SmallRng::from_seed(seed);
        let batch = (0..batch_size)
            .map(|_| SampleStringy::random(r))
            .map(StreamElement::Item)
            .collect();

        let msg = NetworkMessage {
            sender: Coord {
                a: 123,
                b: 34,
                c: 22,
            },
            data: NetworkData::Batch(batch),
        };
        let m2 = msg.clone();

        // group.throughput(Throughput::Bytes(
        //     (DATASET_SIZE * std::mem::size_of::<u32>()) as u64,
        // ));
        let mut scratch = Vec::new();
        group.throughput(criterion::Throughput::Elements(batch_size));
        group.bench_function(
            BenchmarkId::new("bincode_serialize", batch_size),
            move |b| {
                b.iter(|| {
                    bincode_serialize(&mut scratch, black_box(&m2));
                    black_box(&scratch);
                })
            },
        );
        let mut bincode_output = Vec::new();
        bincode_serialize(&mut bincode_output, black_box(&msg));

        group.bench_function(
            BenchmarkId::new("bincode_deserialize", batch_size),
            move |b| {
                b.iter(|| {
                    black_box(bincode_deserialize::<SampleStringy>(black_box(&bincode_output[HEADER_SIZE..])));
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, serde_benchmark);
criterion_main!(benches);
