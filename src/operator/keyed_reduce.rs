use crate::operator::{Data, DataKey, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: Data, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by_reduce<Key: DataKey, Keyer, F>(
        self,
        keyer: Keyer,
        f: F,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: Fn(&Out) -> Key + Send + Clone + 'static,
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.group_by_fold(
            keyer,
            None,
            move |acc, value| match acc {
                None => *acc = Some(value),
                Some(acc) => f(acc, value),
            },
            move |acc1, acc2| match acc1 {
                None => *acc1 = acc2,
                Some(acc1) => match acc2 {
                    None => {}
                    Some(acc2) => f2(acc1, acc2),
                },
            },
        )
        .map(|(_, value)| value.unwrap())
    }
}

impl<Key: DataKey, Out: Data, OperatorChain> KeyedStream<Key, Out, OperatorChain>
where
    OperatorChain: Operator<KeyValue<Key, Out>> + 'static,
{
    pub fn reduce<F>(self, f: F) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => *acc = Some(value),
            Some(acc) => f(acc, value),
        })
        .map(|(_, value)| value.unwrap())
    }
}
