use crate::operator::{ConcatElement, ExchangeData, ExchangeDataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream, KeyedWindowedStream, Stream, WindowedStream};

impl<Key: ExchangeDataKey, Out: ExchangeData, Out2: ExchangeData, WindowDescr, OperatorChain>
    KeyedWindowedStream<Key, Out, OperatorChain, ConcatElement<Out, Out2>, WindowDescr>
where
    WindowDescr: WindowDescription<Key, ConcatElement<Out, Out2>> + Clone + 'static,
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn join<OperatorChain2>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        OperatorChain2: Operator<KeyValue<Key, Out2>> + 'static,
    {
        // concatenate the two streams and apply the window
        self.inner
            .map_concat(right)
            .window(self.descr)
            .add_generic_window_operator("WindowJoin", move |window| {
                // divide the elements coming from the left stream from the elements
                // coming from the right stream
                let (left, right) = window
                    .items()
                    .partition::<Vec<_>, _>(|x| matches!(x, ConcatElement::Left(_)));

                // calculate all the pairs of elements in the current window
                let mut res = Vec::new();
                for l in left.into_iter() {
                    for r in right.iter() {
                        match (l, r) {
                            (ConcatElement::Left(l), ConcatElement::Right(r)) => {
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

impl<Out: ExchangeData, Out2: ExchangeData, WindowDescr, OperatorChain>
    WindowedStream<Out, OperatorChain, ConcatElement<Out, Out2>, WindowDescr>
where
    WindowDescr: WindowDescription<(), ConcatElement<Out, Out2>> + Clone + 'static,
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
            .drop_key()
    }
}
