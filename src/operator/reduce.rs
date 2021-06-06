use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;

impl<Out: ExchangeData, OperatorChain> Stream<Out, OperatorChain>
where
    OperatorChain: Operator<Out> + 'static,
{
    pub fn reduce<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        self.fold(None, move |acc, value| match acc {
            None => *acc = Some(value),
            Some(acc) => f(acc, value),
        })
        .map(|value| value.unwrap())
    }

    pub fn reduce_assoc<F>(self, f: F) -> Stream<Out, impl Operator<Out>>
    where
        F: Fn(&mut Out, Out) + Send + Clone + 'static,
    {
        let f2 = f.clone();

        self.fold_assoc(
            None,
            move |acc, value| match acc {
                None => *acc = Some(value),
                Some(acc) => f(acc, value),
            },
            move |acc1, acc2| match acc1 {
                None => *acc1 = acc2,
                Some(acc1) => {
                    if let Some(acc2) = acc2 {
                        f2(acc1, acc2)
                    }
                }
            },
        )
        .map(|value| value.unwrap())
    }
}
