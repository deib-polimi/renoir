use renoir::prelude::*;

fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let conf = RuntimeConfig::local(4).unwrap();

    let ctx = StreamContext::new(conf.clone());

    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();
    eprintln!("Writing to {}", dir_path.display());

    // Write to multiple files in parallel
    let mut path = dir_path.clone();
    ctx.stream_par_iter(0..100)
        .map(|i| (i, format!("{i:08x}")))
        .write_csv(
            move |i| {
                path.push(format!("{i:03}.csv"));
                path
            },
            false,
        );

    ctx.execute_blocking();

    let ctx = StreamContext::new(conf);
    let mut path = dir_path;
    path.push("001.csv");
    ctx.stream_csv::<(i32, String)>(path)
        .for_each(|t| println!("{t:?}"));

    ctx.execute_blocking();
}
