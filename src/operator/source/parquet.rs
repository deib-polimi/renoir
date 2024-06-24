use std::{fs::File, path::PathBuf};

use arrow::array::{cast::AsArray, types::ArrowPrimitiveType, Array, RecordBatch};
use parquet::arrow::arrow_reader::{ArrowReaderBuilder, ParquetRecordBatchReader};

use crate::{
    operator::{Operator, StreamElement},
    prelude::*,
    structure::{BlockStructure, OperatorKind, OperatorStructure},
    Stream,
};

enum State {
    Running,
    Ended,
}

pub struct ParquetSource {
    path: PathBuf,
    reader: Option<ParquetRecordBatchReader>,
    state: State,
}

impl Clone for ParquetSource {
    fn clone(&self) -> Self {
        panic!("Cannot replicate ParquetSource with replication = 1")
    }
}

impl Operator for ParquetSource {
    type Out = RecordBatch;

    fn setup(&mut self, _metadata: &mut crate::ExecutionMetadata) {
        let file = File::open(&self.path).expect("failed to open file");
        let reader = ArrowReaderBuilder::try_new(file)
            .expect("failed to create arrow reader")
            .with_batch_size(1024);
        let reader = reader.build().expect("failed to build arrow reader");
        self.reader = Some(reader);
    }

    fn next(&mut self) -> StreamElement<Self::Out> {
        let r = self.reader.as_mut().unwrap();
        if let Some(batch) = r.next() {
            return StreamElement::Item(batch.expect("failed to build RecordBatch"));
        }

        match self.state {
            State::Running => {
                self.state = State::Ended;
                StreamElement::FlushAndRestart
            }
            State::Ended => StreamElement::Terminate,
        }
    }

    fn structure(&self) -> crate::structure::BlockStructure {
        let mut operator = OperatorStructure::new::<Self::Out, _>("ParquetSource");
        operator.kind = OperatorKind::Source;
        BlockStructure::default().add_operator(operator)
    }
}

impl std::fmt::Display for ParquetSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Parquet -> ")
    }
}

impl Source for ParquetSource {
    fn replication(&self) -> Replication {
        Replication::One
    }
}

impl crate::StreamContext {
    /// Convenience method, creates a `CsvSource` and makes a stream using `StreamContext::stream`
    pub fn stream_parquet_one(&self, path: impl Into<PathBuf>) -> Stream<ParquetSource> {
        let source = ParquetSource {
            path: path.into(),
            reader: None,
            state: State::Running,
        };

        self.stream(source)
    }
}

impl<Op> Stream<Op>
where
    Op: Operator<Out = RecordBatch> + 'static,
{
    pub fn to_rows<T>(self) -> Stream<impl Operator<Out = Result<T::Native, FromRecordBatchError>>>
    where
        T: FromRecordBatchRow + Send + 'static,
        T::Native: Send,
    {
        self.flat_map(|batch| {
            let mut i = 0;
            let num_rows = batch.num_rows();
            std::iter::from_fn(move || {
                if i < num_rows {
                    let r = T::from_record_batch_row(&batch, i);
                    i += 1;
                    Some(r)
                } else {
                    None
                }
            })
        })
    }
}

pub trait FromRecordBatchRow: Sized {
    type Native;
    fn from_record_batch_row(
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Self::Native, FromRecordBatchError>;
    fn is_compatible(batch: &RecordBatch) -> bool;
}

// Helper function to get a value from a column
fn get_value<T: ArrowPrimitiveType + 'static>(
    batch: &RecordBatch,
    col: usize,
    row: usize,
) -> Result<T::Native, FromRecordBatchError>
where
    T::Native: Clone,
{
    batch
        .column(col)
        .as_primitive_opt::<T>()
        .ok_or_else(|| FromRecordBatchError::IncompatibleTypes(col))
        .map(|array| array.value(row).clone())
}

macro_rules! from_record_batch_tuple {
    ($($id:ident, )+) => {

    // Implementations for tuples
    impl<$($id: ArrowPrimitiveType, )+> FromRecordBatchRow for ($($id, )+)
    {
        type Native = ($($id::Native, )+);

        fn from_record_batch_row(batch: &RecordBatch, row: usize) -> Result<Self::Native, FromRecordBatchError> {
            let mut idx = 0;

            Ok((
                $(get_value::<$id>(batch, { idx += 1; idx - 1}, row)?,)+
            ))
        }

        fn is_compatible(batch: &RecordBatch) -> bool {
            let count = const {
                let mut cnt = 0;
                $(
                    let _ : $id;
                    cnt += 1;
                )+
                cnt
            };
            let mut idx = 0;
            batch.num_columns() == count
                $(&& batch.column({ idx += 1; idx - 1}).data_type() == &$id::DATA_TYPE)+
        }
    }

    };
}

from_record_batch_tuple!(A0,);
from_record_batch_tuple!(A0, A1,);
from_record_batch_tuple!(A0, A1, A2,);
from_record_batch_tuple!(A0, A1, A2, A3,);
from_record_batch_tuple!(A0, A1, A2, A3, A4,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6, A7,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6, A7, A8,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11,);
from_record_batch_tuple!(A0, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12,);

#[derive(Debug, thiserror::Error)]
pub enum FromRecordBatchError {
    #[error("type cannot be converted to primitive")]
    InvalidType,
    #[error("type does not match at column {0}")]
    IncompatibleTypes(usize),
    #[error("index out of bounds")]
    OutOfBounds,
}
