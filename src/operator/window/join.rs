use crate::operator::window::generic_operator::GenericWindowOperator;
use crate::operator::{Data, DataKey, Operator, WindowDescription};
use crate::stream::{KeyValue, KeyedStream};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum JoinElement<A, B> {
    Left(A),
    Right(B),
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + Send + 'static,
{
    pub fn window_join<Out2: Data, OperatorChain2, WindowDescr>(
        self,
        right: KeyedStream<Key, Out2, OperatorChain2>,
        descr: WindowDescr,
    ) -> KeyedStream<Key, (Out, Out2), impl Operator<KeyValue<Key, (Out, Out2)>>>
    where
        WindowDescr: WindowDescription<Key, JoinElement<Out, Out2>> + Clone + 'static,
        OperatorChain2: Operator<KeyValue<Key, Out2>> + Send + 'static,
    {
        // map the left and right streams to the same type
        let left = self.map(|(_, x)| JoinElement::<_, Out2>::Left(x));
        let right = right.map(|(_, x)| JoinElement::<Out, _>::Right(x));

        // concatenate the two streams and apply the window
        let windowed_stream = left.concat(right).window(descr);
        let stream = windowed_stream.inner;
        let descr = windowed_stream.descr;

        stream
            .add_operator(|prev| {
                GenericWindowOperator::new("Join", prev, descr, move |window| {
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
            })
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::EnvironmentConfig;
    use crate::environment::StreamEnvironment;
    use crate::operator::{source, Timestamp, TumblingEventTimeWindow};

    #[test]
    fn window_join() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(4));
        let source1 = source::EventTimeIteratorSource::new(
            (0..10u8).map(|x| (x, Timestamp::from_secs(x.into()))),
            |x, ts| {
                if x % 2 == 1 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let source2 = source::EventTimeIteratorSource::new(
            (0..10u8).map(|x| (x, Timestamp::from_secs(x.into()))),
            |x, ts| {
                if x % 2 == 0 {
                    Some(*ts)
                } else {
                    None
                }
            },
        );

        let stream1 = env.stream(source1).shuffle().group_by(|x| x % 2);
        let stream2 = env
            .stream(source2)
            .shuffle()
            .group_by(|x| x % 2)
            .map(|(_, x)| ('a'..'z').nth(x.into()).unwrap());

        let res = stream1
            .window_join(
                stream2,
                TumblingEventTimeWindow::new(Timestamp::from_secs(3)),
            )
            .unkey()
            .collect_vec();
        env.execute();

        let mut res = res.get().unwrap();
        res.sort_unstable();

        let windows = vec![
            vec![0, 2],
            vec![4],
            vec![6, 8],
            vec![1],
            vec![3, 5],
            vec![7],
            vec![9],
        ];

        let mut expected = Vec::new();
        for window in windows.into_iter() {
            for &x in window.iter() {
                for &y in window.iter() {
                    expected.push((x % 2, (x, ('a'..'z').nth(y.into()).unwrap())));
                }
            }
        }
        expected.sort_unstable();

        assert_eq!(res, expected);
    }
}
