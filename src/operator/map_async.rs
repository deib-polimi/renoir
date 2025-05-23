use std::fmt::Display;
use std::vec::IntoIter as VecIter;

use coarsetime::Instant;
use futures::{Future, StreamExt};
use tokio::runtime::Handle;

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
            BatchMode::Adaptive(n, max_delay)
            | BatchMode::Timed {
                max_size: n,
                interval: max_delay,
            } => {
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

pub struct MapAsync<O: Send + 'static, F, Fut, Op>
where
    F: Fn(Op::Out) -> Fut + Send,
    Fut: Future<Output = O> + Send,
    Op: Operator,
{
    prev: Op,
    batcher: Batcher<Op::Out>,
    buffer: Option<VecIter<StreamElement<O>>>,
    buffering: usize,
    rt: Handle,
    f: F,
}

impl<O: Send + 'static, F, Fut, Op> Clone for MapAsync<O, F, Fut, Op>
where
    F: Fn(Op::Out) -> Fut + Send,
    Fut: Future<Output = O> + Send,
    Op: Operator,
    Op::Out: 'static,
    F: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.prev.clone(), self.f.clone(), self.buffering)
    }
}

impl<O: Data, F, Fut, Op> Display for MapAsync<O, F, Fut, Op>
where
    F: Fn(Op::Out) -> Fut + Send,
    Fut: Future<Output = O> + Send,
    Op: Operator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> MapAsync<{} -> {}>",
            self.prev,
            std::any::type_name::<Op::Out>(),
            std::any::type_name::<O>()
        )
    }
}

impl<O: Send + 'static, F, Fut, Op> MapAsync<O, F, Fut, Op>
where
    F: Fn(Op::Out) -> Fut + Send + Clone,
    Fut: Future<Output = O> + Send,
    Op: Operator,
    Op::Out: 'static,
{
    pub(super) fn new(prev: Op, f: F, buffer: usize) -> Self {
        Self {
            prev,
            batcher: Default::default(),
            rt: Handle::current(),
            f: f,
            buffer: Default::default(),
            buffering: buffer,
        }
    }

    fn run_batch(&mut self, b: Vec<StreamElement<Op::Out>>) {
        let result = self.rt.block_on(
            futures::stream::iter(b.into_iter())
                .map(|el| el.map_async(&self.f))
                .buffered(self.buffering)
                .collect::<Vec<_>>(),
        );

        self.buffer = Some(result.into_iter());
    }
}

impl<O: Data, F, Fut, Op> Operator for MapAsync<O, F, Fut, Op>
where
    F: Fn(Op::Out) -> Fut + Send + Clone,
    Fut: Future<Output = O> + Send,
    Op: Operator,
    Op::Out: 'static,
{
    type Out = O;

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.batcher.mode = metadata.batch_mode;
    }

    #[inline]
    fn next(&mut self) -> StreamElement<O> {
        if matches!(self.batcher.mode, BatchMode::Single) {
            let el = self.prev.next();
            return el.map(|item| self.rt.block_on((self.f)(item)));
        }

        loop {
            if let Some(el) = self.buffer.as_mut().and_then(Iterator::next) {
                return el;
            } else {
                self.buffer = None;
            }

            let el = self.prev.next();
            let kind = el.variant();

            if let Some(b) = self.batcher.enqueue(el) {
                self.run_batch(b);
            }

            if matches!(
                kind,
                StreamElement::FlushAndRestart
                    | StreamElement::FlushBatch
                    | StreamElement::Terminate
            ) {
                if let Some(b) = self.batcher.flush() {
                    self.run_batch(b);
                }
            }
        }
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<O, _>("Map"))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::operator::map_async::MapAsync;
    use crate::operator::{Operator, StreamElement};
    use crate::test::{FakeNetworkTopology, FakeOperator};
    use crate::BatchMode;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn async_map_timed_batch() {
        let mut fake_operator = FakeOperator::new(0..20u8);
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Terminate);

        let mut net = FakeNetworkTopology::<u32>::new(0, 0);
        let mut metadata = net.metadata();
        metadata.batch_mode = BatchMode::timed(5, Duration::from_millis(100));

        let mut map = MapAsync::new(
            fake_operator,
            |x| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                x * 2
            },
            4,
        );

        map.setup(&mut metadata);

        eprintln!("starting new thread ");
        std::thread::scope(|s| {
            s.spawn(move || {
                for i in 0..20 {
                    eprintln!("waiting element {i}");
                    let elem = map.next();
                    eprintln!("received element {i}");
                    assert_eq!(elem, StreamElement::Item(i * 2));
                }
                eprintln!("waiting termination stuff");
                assert_eq!(map.next(), StreamElement::FlushAndRestart);
                assert_eq!(map.next(), StreamElement::Terminate);
            });
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn async_map_adaptive_batch() {
        let mut fake_operator = FakeOperator::new(0..20u8);
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Terminate);

        let mut net = FakeNetworkTopology::<u32>::new(0, 0);
        let mut metadata = net.metadata();
        metadata.batch_mode = BatchMode::adaptive(8, Duration::from_millis(100));

        let mut map = MapAsync::new(
            fake_operator,
            |x| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                x * 2
            },
            4,
        );

        map.setup(&mut metadata);

        eprintln!("starting new thread ");
        std::thread::scope(|s| {
            s.spawn(move || {
                for i in 0..20 {
                    eprintln!("waiting element {i}");
                    let elem = map.next();
                    eprintln!("received element {i}");
                    assert_eq!(elem, StreamElement::Item(i * 2));
                }
                eprintln!("waiting termination stuff");
                assert_eq!(map.next(), StreamElement::FlushAndRestart);
                assert_eq!(map.next(), StreamElement::Terminate);
            });
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn async_map_no_batch() {
        let mut fake_operator = FakeOperator::new(0..20u8);
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Terminate);

        let mut net = FakeNetworkTopology::<u32>::new(0, 0);
        let mut metadata = net.metadata();
        metadata.batch_mode = BatchMode::single();

        let mut map = MapAsync::new(
            fake_operator,
            |x| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                x * 2
            },
            4,
        );

        map.setup(&mut metadata);

        eprintln!("starting new thread ");
        std::thread::scope(|s| {
            s.spawn(move || {
                for i in 0..20 {
                    eprintln!("waiting element {i}");
                    let elem = map.next();
                    eprintln!("received element {i}");
                    assert_eq!(elem, StreamElement::Item(i * 2));
                }
                eprintln!("waiting termination stuff");
                assert_eq!(map.next(), StreamElement::FlushAndRestart);
                assert_eq!(map.next(), StreamElement::Terminate);
            });
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn async_map_fixed_batch() {
        let mut fake_operator = FakeOperator::new(0..20u8);
        fake_operator.push(StreamElement::FlushAndRestart);
        fake_operator.push(StreamElement::Terminate);

        let mut net = FakeNetworkTopology::<u32>::new(0, 0);
        let mut metadata = net.metadata();
        metadata.batch_mode = BatchMode::fixed(8);

        let mut map = MapAsync::new(
            fake_operator,
            |x| async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                x * 2
            },
            4,
        );

        map.setup(&mut metadata);

        eprintln!("starting new thread ");
        std::thread::scope(|s| {
            s.spawn(move || {
                for i in 0..20 {
                    eprintln!("waiting element {i}");
                    let elem = map.next();
                    eprintln!("received element {i}");
                    assert_eq!(elem, StreamElement::Item(i * 2));
                }
                eprintln!("waiting termination stuff");
                assert_eq!(map.next(), StreamElement::FlushAndRestart);
                assert_eq!(map.next(), StreamElement::Terminate);
            });
        });
    }
}
