use crate::operator::{ExchangeData, ExchangeDataKey, KeyerFn, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};
use std::ops::{AddAssign, Div};

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by_min_element<Key: ExchangeDataKey, Value: Ord, Keyer, GetValue>(
        self,
        keyer: Keyer,
        get_value: GetValue,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: KeyerFn<Key, Out> + Fn(&Out) -> Key,
        GetValue: KeyerFn<Value, Out> + Fn(&Out) -> Value,
    {
        self.group_by_reduce(keyer, move |out, value| {
            if get_value(&value) < get_value(&out) {
                *out = value;
            }
        })
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by_max_element<Key: ExchangeDataKey, Value: Ord, Keyer, GetValue>(
        self,
        keyer: Keyer,
        get_value: GetValue,
    ) -> KeyedStream<Key, Out, impl Operator<KeyValue<Key, Out>>>
    where
        Keyer: KeyerFn<Key, Out> + Fn(&Out) -> Key,
        GetValue: KeyerFn<Value, Out> + Fn(&Out) -> Value,
    {
        self.group_by_reduce(keyer, move |out, value| {
            if get_value(&value) > get_value(&out) {
                *out = value;
            }
        })
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by_sum<Key: ExchangeDataKey, Value, Keyer, GetValue>(
        self,
        keyer: Keyer,
        get_value: GetValue,
    ) -> KeyedStream<Key, Value, impl Operator<KeyValue<Key, Value>>>
    where
        Keyer: KeyerFn<Key, Out> + Fn(&Out) -> Key,
        GetValue: KeyerFn<Value, Out> + Fn(&Out) -> Value,
        Value: ExchangeData + AddAssign,
    {
        self.group_by_fold(
            keyer,
            None,
            move |acc, value| {
                if let Some(acc) = acc {
                    *acc += get_value(&value);
                } else {
                    *acc = Some(get_value(&value));
                }
            },
            |acc, value| match acc {
                None => *acc = value,
                Some(acc) => {
                    if let Some(value) = value {
                        *acc += value
                    }
                }
            },
        )
        .map(|(_, o)| o.unwrap())
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by_avg<Key: ExchangeDataKey, Value, Keyer, GetValue>(
        self,
        keyer: Keyer,
        get_value: GetValue,
    ) -> KeyedStream<Key, Value, impl Operator<KeyValue<Key, Value>>>
    where
        Keyer: KeyerFn<Key, Out> + Fn(&Out) -> Key,
        GetValue: KeyerFn<Value, Out> + Fn(&Out) -> Value,
        Value: ExchangeData + AddAssign + Div<f64, Output = Value>,
    {
        self.group_by_fold(
            keyer,
            (None, 0usize),
            move |(sum, count), value| {
                *count += 1;
                match sum {
                    Some(sum) => *sum += get_value(&value),
                    None => *sum = Some(get_value(&value)),
                }
            },
            |(sum, count), (local_sum, local_count)| {
                *count += local_count;
                match sum {
                    None => *sum = local_sum,
                    Some(sum) => {
                        if let Some(local_sum) = local_sum {
                            *sum += local_sum;
                        }
                    }
                }
            },
        )
        .map(|(_, (sum, count))| sum.unwrap() / (count as f64))
    }
}

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn group_by_count<Key: ExchangeDataKey, Keyer>(
        self,
        keyer: Keyer,
    ) -> KeyedStream<Key, usize, impl Operator<KeyValue<Key, usize>>>
    where
        Keyer: KeyerFn<Key, Out> + Fn(&Out) -> Key,
    {
        self.group_by_fold(
            keyer,
            0,
            move |count, _| *count += 1,
            |count, local_count| *count += local_count,
        )
    }
}
