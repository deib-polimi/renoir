use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum JoinElement<A, B> {
    Left(A),
    Right(B),
}

impl<Key: DataKey, Out: Data, Out2: Data, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, JoinElement<Out, Out2>, WindowDescr>
where
    WindowDescr: WindowDescription<Key, JoinElement<Out, Out2>> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn join<OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        OperatorChain2: Operator<KeyValue<Key, Out2>> + Send + 'static,
    {
        // map the left and right streams to the same type
        let left = self.inner.map(|(_, x)| JoinElement::Left(x));
        let right = right.map(|(_, x)| JoinElement::Right(x));

        // concatenate the two streams and apply the window
        left.concat(right)
            .window(self.descr)
            .add_generic_window_operator("Join", move |window| {
                // divide the elements coming from the left stream from the elements
                // coming from the right stream
                let (left, right) = window
                    .items()
                    .partition::<Vec<_>, _>(|x| matches!(x, JoinElement::Left(_)));

                // calculate all the pairs of elements in the current window
                let mut res = Vec::new();
                for l in left.into_iter() {
                    for r in right.iter() {
                        match (l, r) {
                            (JoinElement::Left(l), JoinElement::Right(r)) => {
                                res.push((l.clone(), r.clone()))
                            }
                            _ => {
                                unreachable!("Items of left and right streams are partitioned")
                            }
                        }
                    }
                }
                res
            })
            .flatten()
    }
}
