use std::fmt::Display;
use std::vec::IntoIter as VecIter;

use coarsetime::Instant;
use flume::{Receiver, Sender};
use futures::{Future, StreamExt};

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::{Data, Operator, StreamElement};
use crate::scheduler::ExecutionMetadata;
use crate::BatchMode;

#[derive(Debug, Clone)]
pub(super) struct Batcher<T> {
    mode: BatchMode,
    buffer: Vec<StreamElement<T>>,
    last_send: Instant,
}

impl<T> Default for Batcher<T> {
    fn default() -> Self {
        Self {
            mode: Default::default(),
            buffer: Default::default(),
            last_send: Default::default(),
        }
    }
}

impl<T> Batcher<T> {
    /// Put a message in the batch queue, it won't be sent immediately.
    pub(crate) fn enqueue(&mut self, message: StreamElement<T>) -> Option<Vec<StreamElement<T>>> {
        match self.mode {
            BatchMode::Adaptive(n, max_delay) => {
                self.buffer.push(message);
                let timeout_elapsed = self.last_send.elapsed() > max_delay.into();
                if self.buffer.len() >= n.get() || timeout_elapsed {
                    self.flush()
                } else {
                    None
                }
            }
            BatchMode::Fixed(n) => {
                self.buffer.push(message);
                if self.buffer.len() >= n.get() {
                    self.flush()
                } else {
                    None
                }
            }
            BatchMode::Single => Some(vec![message]),
        }
    }

    /// Flush the internal buffer if it's not empty.
    pub(crate) fn flush(&mut self) -> Option<Vec<StreamElement<T>>> {
        if !self.buffer.is_empty() {
            let cap = self.buffer.capacity();
            let new_cap = if self.buffer.len() < cap / 4 {
                cap / 2
            } else {
                cap
            };
            let mut batch = Vec::with_capacity(new_cap);
            std::mem::swap(&mut self.buffer, &mut batch);
            self.last_send = Instant::now();
            Some(batch)
        } else {
            None
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct MapAsync<I, O, F, Fut, PreviousOperators>
where
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send,
    PreviousOperators: Operator<I>,
    I: Data,
{
    prev: PreviousOperators,
    batcher: Batcher<I>,
    buffer: Option<VecIter<StreamElement<O>>>,
    flushing: bool,
    pending: usize,
    f: F,
    #[derivative(Debug = "ignore")]
    i_tx: Sender<Vec<StreamElement<I>>>,
    #[derivative(Debug = "ignore")]
    o_rx: Receiver<Vec<StreamElement<O>>>,
}

impl<I, O, F, Fut, PreviousOperators> Clone for MapAsync<I, O, F, Fut, PreviousOperators>
where
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send,
    PreviousOperators: Operator<I>,
    I: Data,
    O: Data,
    F: Clone,
    PreviousOperators: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.prev.clone(), self.f.clone(), 0)
    }
}

impl<I: Data, O: Data, F, Fut, PreviousOperators> Display
    for MapAsync<I, O, F, Fut, PreviousOperators>
where
    F: Fn(I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send,
    PreviousOperators: Operator<I>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> MapAsync<{} -> {}>",
            self.prev,
            std::any::type_name::<I>(),
            std::any::type_name::<O>()
        )
    }
}

impl<I: Data, O: Data, F, Fut, PreviousOperators> MapAsync<I, O, F, Fut, PreviousOperators>
where
    F: Fn(I) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = O> + Send,
    PreviousOperators: Operator<I>,
{
    pub(super) fn new(prev: PreviousOperators, f: F, _queue: usize) -> Self {
        const CH: usize = 4;
        let (i_tx, i_rx) = flume::bounded::<Vec<StreamElement<I>>>(CH);
        let (o_tx, o_rx) = flume::bounded::<Vec<StreamElement<O>>>(CH);

        let ff = f.clone();
        tokio::spawn(async move {
            while let Ok(b) = i_rx.recv_async().await {
                let v: Vec<_> = futures::stream::iter(b.into_iter())
                    .then(|el| el.map_async(&ff))
                    .collect()
                    .await;

                o_tx.send_async(v).await.unwrap();
            }
        });

        Self {
            prev,
            batcher: Default::default(),
            f,
            flushing: false,
            pending: 0,
            buffer: Default::default(),
            i_tx,
            o_rx,
        }
    }

    fn schedule_batch(&mut self, b: Vec<StreamElement<I>>) {
        match self.i_tx.try_send(b) {
            Ok(()) => self.pending += 1,
            Err(flume::TrySendError::Full(b)) => {
                self.recv_output_batch();
                self.i_tx.send(b).unwrap();
                self.pending += 1
            }
            Err(e) => panic!("{e}"),
        }
    }

    fn recv_output_batch(&mut self) {
        assert!(
            self.pending > 0,
            "map_async trier receiving batches, but pending is equal to 0"
        );
        self.buffer = Some(self.o_rx.recv().unwrap().into_iter());
        self.pending -= 1;
    }
}

impl<I: Data, O: Data, F, Fut, PreviousOperators> Operator<O>
    for MapAsync<I, O, F, Fut, PreviousOperators>
where
    F: Fn(I) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = O> + Send,
    PreviousOperators: Operator<I>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.batcher.mode = metadata.batch_mode;
    }

    #[inline]
    fn next(&mut self) -> StreamElement<O> {
        loop {
            if let Some(el) = self.buffer.as_mut().and_then(Iterator::next) {
                return el;
            } else {
                self.buffer = None;
            }

            if self.flushing && self.pending > 0 {
                self.recv_output_batch();
                continue;
            }
            if self.flushing && self.pending == 0 {
                self.flushing = false;
            }

            let el = self.prev.next();
            let kind = el.take();

            if let Some(b) = self.batcher.enqueue(el) {
                self.schedule_batch(b);
            }

            if matches!(
                kind,
                StreamElement::FlushAndRestart
                    | StreamElement::FlushBatch
                    | StreamElement::Terminate
            ) {
                if let Some(b) = self.batcher.flush() {
                    self.schedule_batch(b);
                }
            }
            if matches!(
                kind,
                StreamElement::FlushAndRestart | StreamElement::Terminate
            ) {
                self.flushing = true;
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("Map"))
    }
}

// #[cfg(test)]
// mod tests {
//     use std::str::FromStr;

//     use crate::operator::map::Map;
//     use crate::operator::{Operator, StreamElement};
//     use crate::test::FakeOperator;

//     #[test]
//     #[cfg(feature = "timestamp")]
//     fn map_stream() {
//         let mut fake_operator = FakeOperator::new(0..10u8);
//         for i in 0..10 {
//             fake_operator.push(StreamElement::Timestamped(i, i as i64));
//         }
//         fake_operator.push(StreamElement::Watermark(100));

//         let map = Map::new(fake_operator, |x| x.to_string());
//         let map = Map::new(map, |x| x + "000");
//         let mut map = Map::new(map, |x| u32::from_str(&x).unwrap());

//         for i in 0..10 {
//             let elem = map.next();
//             assert_eq!(elem, StreamElement::Item(i * 1000));
//         }
//         for i in 0..10 {
//             let elem = map.next();
//             assert_eq!(elem, StreamElement::Timestamped(i * 1000, i as i64));
//         }
//         assert_eq!(map.next(), StreamElement::Watermark(100));
//         assert_eq!(map.next(), StreamElement::Terminate);
//     }
// }
