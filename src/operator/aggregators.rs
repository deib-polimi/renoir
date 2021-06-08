use std::ops::{AddAssign, Div};

use crate::operator::{ExchangeData, ExchangeDataKey, KeyerFn, Operator};
use crate::stream::{KeyValue, KeyedStream, Stream};

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    /// Find, for each partition of the stream, the item with the smallest value.
    ///
    /// The stream is partitioned using the `keyer` function and the value to compare is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: the comparison is done using the value returned by `get_value`, but the resulting
    /// items have the same type as the input.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_min_element(|&n| n % 2, |&n| n)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0), (1, 1)]);
    /// ```
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
    /// Find, for each partition of the stream, the item with the largest value.
    ///
    /// The stream is partitioned using the `keyer` function and the value to compare is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: the comparison is done using the value returned by `get_value`, but the resulting
    /// items have the same type as the input.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_max_element(|&n| n % 2, |&n| n)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 4), (1, 3)]);
    /// ```
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
    /// Find, for each partition of the stream, the sum of the values of the items.
    ///
    /// The stream is partitioned using the `keyer` function and the value to sum is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: this is similar to the SQL: `SELECT SUM(value) ... GROUP BY key`
    ///
    /// **Note**: the type of the result does not have to be a number, any type that implements
    /// `AddAssign` is accepted.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_sum(|&n| n % 2, |&n| n)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_unstable();
    /// assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
    /// ```
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
    /// Find, for each partition of the stream, the average of the values of the items.
    ///
    /// The stream is partitioned using the `keyer` function and the value to average is obtained
    /// with `get_value`.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: this is similar to the SQL: `SELECT AVG(value) ... GROUP BY key`
    ///
    /// **Note**: the type of the result does not have to be a number, any type that implements
    /// `AddAssign` and can be divided by `f64` is accepted.
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_avg(|&n| n % 2, |&n| n as f64)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_by_key(|(k, _)| *k);
    /// assert_eq!(res, vec![(0, (0.0 + 2.0 + 4.0) / 3.0), (1, (1.0 + 3.0) / 2.0)]);
    /// ```
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
    /// Count, for each partition of the stream, the number of items.
    ///
    /// The stream is partitioned using the `keyer` function.
    ///
    /// This operation is associative, therefore the computation is done in parallel before sending
    /// all the elements to the network.
    ///
    /// **Note**: this is similar to the SQL: `SELECT COUNT(*) ... GROUP BY key`
    ///
    /// **Note**: this operator will split the current block.
    ///
    /// ## Example
    /// ```
    /// # use rstream::{StreamEnvironment, EnvironmentConfig};
    /// # use rstream::operator::source::IteratorSource;
    /// # let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
    /// let s = env.stream(IteratorSource::new((0..5)));
    /// let res = s
    ///     .group_by_count(|&n| n % 2)
    ///     .collect_vec();
    ///
    /// env.execute();
    ///
    /// let mut res = res.get().unwrap();
    /// res.sort_by_key(|(k, _)| *k);
    /// assert_eq!(res, vec![(0, 3), (1, 2)]);
    /// ```
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
