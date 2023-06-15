use crate::block::NextStrategy;
use crate::operator::{ExchangeData, Operator};
use crate::stream::Stream;
use std::collections::HashMap;
use std::fmt::{Debug, Display};

use crate::block::{BatchMode, Batcher, BlockStructure, Connection, OperatorStructure};
use crate::network::{Coord, ReceiverEndpoint};
use crate::operator::{KeyerFn, StreamElement};
use crate::scheduler::{BlockId, ExecutionMetadata};

use super::end::BlockSenders;
use super::SimpleStartReceiver;
use crate::operator::start::Start;

#[derive(Clone)]
pub(crate) struct FilterFn<Out>(fn(&Out) -> bool);

impl<Out> FilterFn<Out> {
    fn is_match(&self, item: &Out) -> bool {
        (self.0)(item)
    }
}

impl<Out> Debug for FilterFn<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("FilterFn")
            .field(&std::any::type_name::<Out>())
            .finish()
    }
}

pub struct RouterBuilder<T: ExchangeData, O: Operator<T>> {
    stream: Stream<T, O>,
    routes: Vec<FilterFn<T>>,
}

impl<Out: ExchangeData, OperatorChain: Operator<Out> + 'static> RouterBuilder<Out, OperatorChain> {
    pub(super) fn new(stream: Stream<Out, OperatorChain>) -> Self {
        Self {
            stream,
            routes: Vec::new(),
        }
    }

    pub fn add_route(mut self, filter: fn(&Out) -> bool) -> Self {
        self.routes.push(FilterFn(filter));
        self
    }

    pub fn build(self) -> Vec<Stream<Out, impl Operator<Out>>> {
        self.build_inner()
    }

    pub(crate) fn build_inner(self) -> Vec<Stream<Out, Start<Out, SimpleStartReceiver<Out>>>> {
        // This is needed to maintain the same parallelism of the split block
        let env_lock = self.stream.env.clone();
        let mut env = env_lock.lock();
        let scheduler_requirements = self.stream.block.scheduler_requirements.clone();
        let batch_mode = self.stream.block.batch_mode;
        let block_id = self.stream.block.id;
        let iteration_context = self.stream.block.iteration_ctx.clone();

        let mut new_blocks = (0..self.routes.len())
            .map(|_| {
                env.new_block(
                    Start::single(block_id, iteration_context.last().cloned()),
                    batch_mode,
                    iteration_context.clone(),
                )
            })
            .collect::<Vec<_>>();

        let routes = new_blocks.iter().map(|b| b.id).zip(self.routes).collect();
        let stream = self.stream.add_operator(move |prev| {
            RoutingEnd::new(prev, routes, NextStrategy::only_one(), batch_mode)
        });

        env.close_block(stream.block);

        for new_block in &mut new_blocks {
            env.connect_blocks::<Out>(block_id, new_block.id);
            new_block.scheduler_requirements = scheduler_requirements.clone();
        }

        drop(env);
        new_blocks
            .into_iter()
            .map(|block| Stream {
                block,
                env: env_lock.clone(),
            })
            .collect()
    }
}

#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct RoutingEnd<Out: ExchangeData, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    prev: OperatorChain,
    coord: Option<Coord>,
    next_strategy: NextStrategy<Out, IndexFn>,
    batch_mode: BatchMode,
    #[derivative(Debug = "ignore", Clone(clone_with = "clone_default"))]
    senders: Vec<(ReceiverEndpoint, Batcher<Out>)>,

    endpoints: Vec<Endpoint<Out>>,
    routes: Vec<(BlockId, FilterFn<Out>)>,
}

impl<Out: ExchangeData, OperatorChain, IndexFn> Display for RoutingEnd<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.next_strategy {
            NextStrategy::Random => write!(f, "{} -> RouteShuffle", self.prev),
            NextStrategy::OnlyOne => write!(f, "{} -> RouteOnlyOne", self.prev),
            _ => self.prev.fmt(f),
        }
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> RoutingEnd<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    pub(crate) fn new(
        prev: OperatorChain,
        routes: Vec<(BlockId, FilterFn<Out>)>,
        next_strategy: NextStrategy<Out, IndexFn>,
        batch_mode: BatchMode,
    ) -> Self {
        Self {
            prev,
            coord: None,
            next_strategy,
            batch_mode,
            endpoints: Default::default(),
            routes,
            senders: Default::default(),
        }
    }

    fn setup_endpoints(&mut self) {
        self.senders.sort_unstable_by_key(|s| s.0);
        let mut block_map: HashMap<BlockId, Vec<usize>> =
            self.senders
                .iter()
                .enumerate()
                .fold(HashMap::new(), |mut map, (i, s)| {
                    map.entry(s.0.coord.block_id).or_default().push(i);
                    map
                });

        for (block_id, filter) in self.routes.drain(..) {
            let indexes = block_map
                .remove(&block_id)
                .expect("scheduler connection missing for RoutingEnd");
            let block_senders = BlockSenders { indexes };
            self.endpoints.push(Endpoint {
                block_id,
                filter,
                block_senders,
            });
        }

        assert!(self.routes.is_empty());
        assert!(block_map.is_empty());
    }
}

#[derive(Clone)]
struct Endpoint<Out> {
    block_id: BlockId,
    filter: FilterFn<Out>,
    block_senders: BlockSenders,
}

impl<Out: Debug> Debug for Endpoint<Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("block_id", &self.block_id)
            .field("filter", &std::any::type_name::<Out>())
            .field("senders", &self.block_senders)
            .finish()
    }
}

impl<Out: ExchangeData, OperatorChain, IndexFn> Operator<()>
    for RoutingEnd<Out, OperatorChain, IndexFn>
where
    IndexFn: KeyerFn<u64, Out>,
    OperatorChain: Operator<Out>,
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);

        let senders = metadata.network.get_senders(metadata.coord);
        // remove the ignored destinations
        self.senders = senders
            .into_iter()
            .map(|(coord, sender)| (coord, Batcher::new(sender, self.batch_mode, metadata.coord)))
            .collect();

        self.setup_endpoints();

        self.coord = Some(metadata.coord);
    }

    fn next(&mut self) -> StreamElement<()> {
        assert!(
            self.routes.is_empty(),
            "RoutingEnd still has routes to be setup!"
        );
        let message = self.prev.next();
        let to_return = message.take();
        match &message {
            // Broadcast messages
            StreamElement::Watermark(_)
            | StreamElement::Terminate
            | StreamElement::FlushAndRestart => {
                for e in self.endpoints.iter() {
                    for &sender_idx in e.block_senders.indexes.iter() {
                        let sender = &mut self.senders[sender_idx];
                        sender.1.enqueue(message.clone());
                    }
                }
            }
            // Direct messages
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                let index = self.next_strategy.index(item);

                let mut sent = false;
                for e in self.endpoints.iter_mut() {
                    if e.filter.is_match(item) {
                        let sender_idx = e.block_senders.indexes[index];
                        self.senders[sender_idx].1.enqueue(message);
                        sent = true;
                        break;
                    }
                }

                if !sent {
                    log::trace!("router ignoring message");
                }
            }
            StreamElement::FlushBatch => {}
        };

        // Flushing messages
        match to_return {
            StreamElement::FlushAndRestart | StreamElement::FlushBatch => {
                for (_, batcher) in self.senders.iter_mut() {
                    batcher.flush();
                }
            }
            StreamElement::Terminate => {
                log::trace!(
                    "routing_end terminate {}, closing {} channels",
                    self.coord.unwrap(),
                    self.senders.len()
                );
                for (_, batcher) in self.senders.drain(..) {
                    batcher.end();
                }
            }
            _ => {}
        }

        to_return
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("RoutingEnd");
        for e in &self.endpoints {
            if !e.block_senders.indexes.is_empty() {
                let block_id = self.senders[e.block_senders.indexes[0]].0.coord.block_id;
                operator
                    .connections
                    .push(Connection::new::<Out, _>(block_id, &self.next_strategy));
            }
        }
        self.prev.structure().add_operator(operator)
    }
}

fn clone_default<T>(_: &T) -> T
where
    T: Default,
{
    T::default()
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    #[test]
    #[allow(clippy::identity_op)]
    fn test_route() {
        let mut env = StreamEnvironment::new(EnvironmentConfig::local(1));
        let s = env.stream_iter(0..10);

        let mut routes = s
            .route()
            .add_route(|&i| i < 5)
            .add_route(|&i| i % 2 == 0)
            .build()
            .into_iter();
        assert_eq!(routes.len(), 2);

        // 0 1 2 3 4
        routes
            .next()
            .unwrap()
            .for_each(|i| eprintln!("route1: {i}"));
        // 6 8
        routes
            .next()
            .unwrap()
            .for_each(|i| eprintln!("route2: {i}"));
        // 5 7 9 ignored

        env.execute_blocking();
    }
}
