use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};
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
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn join<OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        OperatorChain2: Operator<KeyValue<Key, Out2>> + 'static,
    {
        // map the left and right streams to the same type
        let left = self.inner.map(|(_, x)| JoinElement::Left(x));
        let right = right.map(|(_, x)| JoinElement::Right(x));

        // concatenate the two streams and apply the window
        left.concat(right)
            .window(self.descr)
            .add_generic_window_operator("WindowJoin", move |window| {
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

impl<Out: Data, Out2: Data, WindowDescr, OperatorChain>
    WindowedStream<Out, OperatorChain, JoinElement<Out, Out2>, WindowDescr>
where
    WindowDescr: WindowDescription<(), JoinElement<Out, Out2>> + Clone + 'static,
    OperatorChain: Operator<KeyValue<(), Out>> + 'static,
{
    pub fn join<OperatorChain2>(
        self,
        right: Stream<Out2, OperatorChain2>,
    ) -> Stream<(Out, Out2), impl Operator<(Out, Out2)>>
    where
        OperatorChain2: Operator<Out2> + 'static,
    {
        self.inner
            .join(right.max_parallelism(1).key_by(|_| ()))
            .unkey()
            .map(|(_, x)| x)
    }
}
