use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossbeam_channel::{bounded, Receiver, Select};
use rstream::network::{Coord, NetworkReceiver, ReceiverEndpoint};

const CHANNEL_CAPACITY: usize = 10;

fn raw_run(size: usize, body: impl FnOnce(Receiver<usize>)) {
    let (sender, receiver) = bounded(CHANNEL_CAPACITY);
    let join_handle = std::thread::Builder::new()
        .name("recv producer".to_string())
        .spawn(move || {
            for i in 0..size {
                sender.send(i).unwrap();
            }
        })
        .unwrap();
    body(receiver);
    join_handle.join().unwrap();
}

fn raw_recv(size: usize) {
    raw_run(size, |recv| while recv.recv().is_ok() {});
}

fn raw_select(size: usize) {
    raw_run(size, |recv| {
        for _ in 0..size {
            let mut select = Select::new();
            select.recv(&recv);
            let _ = select.ready();
            recv.recv().unwrap();
        }
    });
}

fn run(size: usize, body: impl FnOnce(NetworkReceiver<usize>)) {
    let mut receiver = NetworkReceiver::new(ReceiverEndpoint::new(Coord::new(0, 0, 0), 0));
    let sender = receiver.sender().unwrap();
    let join_handle = std::thread::Builder::new()
        .name("recv producer".to_string())
        .spawn(move || {
            for i in 0..size {
                sender.send(i).unwrap();
            }
        })
        .unwrap();
    body(receiver);
    join_handle.join().unwrap();
}

fn recv(size: usize) {
    run(size, |recv| while recv.recv().is_ok() {});
}

fn select(size: usize) {
    run(size, |recv| {
        let recv = vec![recv];
        for _ in 0..size {
            NetworkReceiver::select_any(recv.iter()).result.unwrap();
        }
    });
}

fn recv_vs_select_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("recv_vs_select");
    const ELEMENTS: usize = 100_000;
    group.throughput(Throughput::Elements(ELEMENTS as u64));
    group.bench_function("raw_recv", |b| b.iter(|| raw_recv(black_box(ELEMENTS))));
    group.bench_function("raw_select", |b| b.iter(|| raw_select(black_box(ELEMENTS))));
    group.bench_function("recv", |b| b.iter(|| recv(black_box(ELEMENTS))));
    group.bench_function("select", |b| b.iter(|| select(black_box(ELEMENTS))));
    group.finish();
}

criterion_group!(benches, recv_vs_select_benchmark);
criterion_main!(benches);
