use std::time::Instant;

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
        .group_by_fold(
            |(v1, _)| *v1,
            Vec::new(),
            |edges, e| edges.push(e.1),
            |x, mut y| x.append(&mut y),
        )
        // generate all the possible triangles
        .flat_map(|(_v1, edges)| {
            let mut triangles = Vec::new();
            for i in 0..edges.len() {
                for j in 0..i {
                    let v2 = edges[i].min(edges[j]);
                    let v3 = edges[i].max(edges[j]);
                    triangles.push((v2, v3));
                }
            }
            triangles
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
