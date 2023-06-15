use std::time::Instant;

use itertools::Itertools;

use noir::prelude::*;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let (config, args) = EnvironmentConfig::from_args();
    if args.len() != 1 {
        panic!("Pass the dataset path as an argument");
    }
    let path = &args[0];

    let mut env = StreamEnvironment::new(config);

    env.spawn_remote_workers();

    let source = CsvSource::<(u32, u32)>::new(path).has_headers(false);
    let mut edges = env
        .stream(source)
        // make sure v1 is less than v2
        .map(|(v1, v2)| (v1.min(v2), v1.max(v2)))
        .split(2);

    let triangles = edges
        .pop()
        .unwrap()
        .group_by(|(v1, _)| *v1)
        // generate all the triangles incrementally
        .rich_flat_map({
            let mut old_vertices = Vec::new();
            move |(_, (_, y))| {
                let new_triangles = old_vertices
                    .iter()
                    .map(|&x: &u32| (x.min(y), x.max(y)))
                    .collect_vec();
                old_vertices.push(y);
                new_triangles
            }
        })
        .unkey()
        .map(|(v1, (v2, v3))| (v1, v2, v3))
        // keep only valid triangles
        .join(
            edges.pop().unwrap(),
            |(_v1, v2, v3)| (*v2, *v3),
            |(v1, v2)| (*v1, *v2),
        )
        .unkey()
        // count the triangles
        .fold_assoc(0_u64, |x, _| *x += 1, |x, y| *x += y);

    let result = triangles.collect_vec();

    let start = Instant::now();
    env.execute_blocking();
    let elapsed = start.elapsed();

    if let Some(res) = result.get() {
        assert!(res.len() <= 1);
        let triangles = if let Some(x) = res.first() { *x } else { 0 };
        eprintln!("Output: {triangles:?}");
    }
    eprintln!("Elapsed: {elapsed:?}");
}
