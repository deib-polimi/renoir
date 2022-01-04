use std::time::{Duration, SystemTime};

use noir::operator::source::ParallelIteratorSource;
use noir::{BatchMode, EnvironmentConfig, StreamEnvironment};

use latency::repeat;

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 5 {
        panic!(
            "\n\nUsage: num_items items_per_sec batch_size batch_timeout num_threads\n\
            - num_items: number of items to put in the stream\n\
            - items_per_sec: number of items to generate per second (0 = as fast as possible)\n\
            - batch_size: the size of the BatchMode::Fixed or BatchMode::Adaptive\n\
            - batch_timeout: 0 => BatchMode::Fixed, n > 0 => BatchMode::Adaptive (in ms)\n\
            - num_threads: number of generating threads\n\n"
        );
    }

    let num_items: usize = args[0].parse().expect("invalid num_items");
    let items_per_sec: u64 = args[1].parse().expect("invalid items_per_sec");
    let batch_size: usize = args[2].parse().expect("invalid batch_size");
    let batch_timeout: u64 = args[3].parse().expect("invalid batch_timeout");
    let num_threads: usize = args[4].parse().expect("invalid num_threads");

    assert!(num_items >= 1, "num_items must be at least 1");
    assert!(num_threads >= 1, "num_threads must be at least 1");

    let batch_mode = if batch_timeout == 0 {
        BatchMode::fixed(batch_size)
    } else {
        BatchMode::adaptive(batch_size, Duration::from_millis(batch_timeout))
    };

    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = ParallelIteratorSource::new(move |id, _num_replica| {
        let iter = if id < num_threads {
            let to_generate = num_items / num_threads;
            (to_generate * id)..(to_generate * (id + 1))
        } else {
            0..0
        };
        iter.map(move |i| {
            if items_per_sec > 0 {
                std::thread::sleep(Duration::from_micros(
                    1_000_000 * num_threads as u64 / items_per_sec,
                ));
            }
            i
        })
    });
    let stream = env.stream(source).batch_mode(BatchMode::fixed(1));

    // first shuffle: move the items to a predictable host
    let stream = stream
        .group_by(|_| 0)
        .map(|(_, i)| (0, i, SystemTime::now()))
        .batch_mode(batch_mode)
        .drop_key();

    // n-1 shuffles to accumulate latency
    let stream = repeat!(
        4,
        stream,
        map(|(i, n, t)| (i + 1, n, t))
            .group_by(|&(i, _, _)| i)
            .drop_key()
    );

    // final shuffle back to the first host
    let stream = stream
        .map(|(i, n, t)| (i + 1, n, t))
        .group_by(|_| 0)
        .drop_key();

    // compute the time durations; time are accurate because it's the same host (and we ignore clock
    // skews)
    stream.for_each(|(i, n, start)| {
        let duration = start.elapsed().expect("Clock skewed");
        // num steps,item index,latency
        eprintln!("{},{},{}", i, n, duration.as_nanos());
    });

    env.execute();
}
