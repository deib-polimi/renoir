mod fold;
// mod columnar;
pub(super) use fold::{Fold, FoldFirst};

mod collect_vec;
mod count;
mod join;
mod max;
mod min;
mod nth;
mod sum;
#[cfg(feature = "parquet")]
mod to_arrow;
