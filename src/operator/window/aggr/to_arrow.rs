use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::json::ReaderBuilder;
use serde::Serialize;

use super::super::*;
use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyedStream, WindowedStream};

struct ToArrow<T> {
    vec: Vec<T>,
    schema: Arc<Schema>,
}

impl<T> Clone for ToArrow<T> {
    fn clone(&self) -> Self {
        Self {
            vec: Vec::new(),
            schema: self.schema.clone(),
        }
    }
}

impl<T: Clone + Serialize + Send + 'static> WindowAccumulator for ToArrow<T> {
    type In = T;
    type Out = RecordBatch;

    #[inline]
    fn process(&mut self, el: Self::In) {
        self.vec.push(el);
    }

    #[inline]
    fn output(self) -> Self::Out {
        if self.vec.is_empty() {
            RecordBatch::new_empty(self.schema)
        } else {
            let mut decoder = ReaderBuilder::new(self.schema)
                .build_decoder()
                .expect("arrow error");
            decoder.serialize(&self.vec).expect("arrow error");
            decoder
                .flush()
                .map(|o| o.expect("batch must contain some elements"))
                .expect("arrow error")
        }
    }
}

impl<Key, Out, WindowDescr, OperatorChain> WindowedStream<OperatorChain, Out, WindowDescr>
where
    WindowDescr: WindowDescription<Out>,
    OperatorChain: Operator<Out = (Key, Out)> + 'static,
    Key: DataKey,
    Out: Serialize + Data,
{
    /// Prefer other aggregators if possible as they don't save all elements
    pub fn to_arrow(
        self,
        schema: Arc<Schema>,
    ) -> KeyedStream<impl Operator<Out = (Key, RecordBatch)>> {
        let acc = ToArrow {
            vec: Default::default(),
            schema,
        };
        self.add_window_operator("ToArrow", acc)
    }
}

#[cfg(test)]
mod tests {
    use crate::StreamContext;

    use super::*;
    use arrow::datatypes::{DataType, Field};
    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestStruct {
        pub uint: u32,
        pub double: f64,
        pub text: String,
    }

    impl TestStruct {
        pub fn schema() -> Schema {
            Schema::new(vec![
                Field::new("uint", DataType::UInt32, false),
                Field::new("double", DataType::Float64, false),
                Field::new("text", DataType::Utf8, false),
            ])
        }
    }

    fn gen(i: u32) -> TestStruct {
        TestStruct {
            uint: i,
            double: (i as f64).sqrt(),
            text: format!("{:x}", i * i * i),
        }
    }

    #[test]
    fn to_arrow() {
        let schema = Arc::new(TestStruct::schema());

        let ctx = StreamContext::new_local();

        ctx.stream_iter(0..200)
            .map(gen)
            .window_all(CountWindow::tumbling(32))
            .to_arrow(schema)
            .for_each(|rb| {
                eprintln!("{:?}", rb);
            });

        ctx.execute_blocking();
    }
}
