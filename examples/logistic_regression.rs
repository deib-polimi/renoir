//! This code is pretty broken and breaks the CI, commenting it for now :^)
//!
//! Most notable problems:
//!
//! - replay is pretty broken (the folding functions seem wrong)
//! - two steps computation shouldn't be needed (and the second step is very broken)

fn main() {}

// use std::time::Instant;
//
// use serde::{Deserialize, Serialize};
//
// use noir::operator::source::CsvSource;
// use noir::EnvironmentConfig;
// use noir::StreamEnvironment;
//
// #[derive(Serialize, Deserialize, Clone, Copy, Debug)]
// struct LabeledPoint {
//     #[serde(rename = "LABEL")]
//     label: f64,
//     #[serde(rename = "FEATURE1")]
//     feature1: f64,
//     #[serde(rename = "FEATURE2")]
//     feature2: f64,
//     #[serde(rename = "FEATURE3")]
//     feature3: f64,
// }
//
// impl LabeledPoint {
//     fn get_updated_features(&self, weight: &[f64]) -> Vec<f64> {
//         let lr = 0.01;
//         let features = vec![self.feature1, self.feature2, self.feature3];
//         let hyp_function = logistic_function(&features, weight) - self.label;
//         features
//             .clone()
//             .iter()
//             .map(|x| x * hyp_function * lr)
//             .collect()
//     }
//     fn compute_cost(&self, weight: &[f64]) -> f64 {
//         let features = vec![self.feature1, self.feature2, self.feature3];
//         let hyp_function = logistic_function(&features, weight);
//         let comp1 = self.label * hyp_function.ln();
//         let comp2 = (1.0 - self.label) * (1.0 - hyp_function).ln();
//         -comp1 - comp2
//     }
// }
//
// #[derive(Serialize, Deserialize, Clone, Debug)]
// struct State {
//     weight: Vec<f64>,
//     cost: f64,
//     old_cost: f64,
//     count: f64,
//     iterations: usize,
// }
//
// impl std::fmt::Display for LabeledPoint {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{:?} {:?} {:?} {:?}",
//             self.label, self.feature1, self.feature2, self.feature3
//         )
//     }
// }
// impl std::fmt::Display for State {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{:?} {:?} {:?} {:?}",
//             self.weight, self.cost, self.old_cost, self.count
//         )
//     }
// }
//
// fn main() {
//     let (config, args) = EnvironmentConfig::from_args();
//     if args.len() != 3 {
//         panic!("Pass the path to the dataset and the maximum number of iterations");
//     }
//     let path = &args[0];
//     let num_iters: usize = args[1].parse().expect("Invalid number of iterations");
//
//     let mut env = StreamEnvironment::new(config);
//     env.spawn_remote_workers();
//
//     let state = State {
//         weight: vec![0.0, 0.0, 0.0],
//         cost: 0.0,
//         old_cost: 1000.0,
//         count: 0.0,
//         iterations: 0,
//     };
//
//     let source = CsvSource::<LabeledPoint>::new(path).has_headers(true);
//     let res = env
//         .stream(source)
//         .replay(
//             num_iters,
//             state,
//             |s, state| {
//                 s.map(|x: LabeledPoint| {
//                     (
//                         x.get_updated_features(&state.get().weight),
//                         1.0,
//                         x.compute_cost(&state.get().weight),
//                     )
//                 })
//                 .group_by(|_| ())
//                 .reduce(|(features, count, cost), (features2, count2, cost2)| {
//                     features[0] += features2[0];
//                     features[1] += features2[1];
//                     features[2] += features2[2];
//                     *count += count2;
//                     *cost += cost2;
//                 })
//                 .drop_key()
//             },
//             |update, p| update = p,
//             |s, (f, count, cost)| {
//                 s.weight = vec![s.weight[0] - f[0], s.weight[1] - f[1], s.weight[2] - f[2]];
//                 s.count = count;
//                 s.cost = cost / count;
//             },
//             |s| {
//                 let max_difference = 0.001;
//                 let difference = s.cost - s.old_cost;
//                 //println!("Cost: {}", s.cost);
//                 s.old_cost = s.cost;
//                 iteration += 1;
//                 println!("Iteration: {}", iteration);
//                 difference.abs() > max_difference
//             },
//         )
//         .collect_vec();
//
//     let start = Instant::now();
//     env.execute_blocking();
//     let state = &res[0];
//     env = StreamEnvironment::new();
//     env.spawn_remote_workers();
//     let accuracy_vector = env
//         .map(|x: LabeledPoint| vec![x.label, predict(&x, &state.weight)])
//         .filter(|x| x[0] == x[1])
//         .collect_vec();
//
//     env.execute_blocking();
//
//     let accuracy: f64 = accuracy_vector.iter().count() as f64 / state.count as f64;
//     println!("Accuracy: {}", accuracy);
//     println!("Elapsed time: {}", now.elapsed().as_millis());
//     finalize();
// }
//
// fn matrix_mult(first_matrix: &[f64], second_matrix: &[f64]) -> f64 {
//     let length = first_matrix.len();
//     let mut mult: f64 = 0.0;
//     for i in 0..length {
//         mult = mult + first_matrix[i] * second_matrix[i];
//     }
//     mult
// }
//
// fn logistic_function(features: &[f64], weight: &[f64]) -> f64 {
//     let mut mult: f64 = matrix_mult(&features, &weight);
//     mult = (mult.min(10.0)).max(-10.0);
//     mult = -mult;
//     let hyp_function = 1.0 / (1.0 + mult.exp());
//     hyp_function
// }
//
// fn predict(point: &LabeledPoint, weights: &[f64]) -> f64 {
//     let features = vec![point.feature1, point.feature2, point.feature3];
//     let pred_prob = logistic_function(&features, &weights);
//     let mut prediction = 0.0;
//     if pred_prob >= 0.5 {
//         prediction = 1.0;
//     }
//     prediction
// }
