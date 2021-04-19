use std::any::TypeId;
use std::collections::VecDeque;

use crate::block::{BlockStructure, OperatorReceiver, OperatorStructure};
use crate::network::{Coord, NetworkMessage, NetworkReceiver, ReceiverEndpoint};
use crate::operator::{Data, Operator, StreamElement, Timestamp};
use crate::scheduler::ExecutionMetadata;
use crate::stream::BlockId;
use itertools::Itertools;

#[derive(Clone, Debug, Default)]
/// Handle watermarks coming from multiple replicas.
/// A watermark with timestamp `ts` is safe to be passed downstream if and only if,
/// for every previous replica, a watermark with timestamp greater or equal to `ts`
/// has already been received
struct WatermarkFrontier {
    /// Watermark with largest timestamp received, for each replica
    largest_watermark: Vec<Option<Timestamp>>,
    /// List of previous replicas
    prev_replicas: Vec<Coord>,
}

impl WatermarkFrontier {
    fn new(prev_replicas: Vec<Coord>) -> Self {
        Self {
            largest_watermark: vec![None; prev_replicas.len()],
            prev_replicas,
        }
    }

    /// Return the current maximum safe timestamp
    fn get_frontier(&self) -> Option<Timestamp> {
        // if at least one replica has not sent a watermark yet, return None
        let missing_watermarks = self.largest_watermark.iter().any(|x| x.is_none());
        if missing_watermarks {
            None
        } else {
            // the current frontier is the minimum watermark received
            Some(
                self.largest_watermark
                    .iter()
                    .map(|x| x.unwrap())
                    .min()
                    .unwrap(),
            )
        }
    }

    /// Update the frontier, return `Some(ts)` if timestamp `ts` is now safe
    fn update(&mut self, coord: Coord, ts: Timestamp) -> Option<Timestamp> {
        let old_frontier = self.get_frontier();

        // find position of the replica in the list of replicas
        let replica_idx = self
            .prev_replicas
            .iter()
            .find_position(|&&prev| prev == coord)
            .unwrap()
            .0;

        // update watermark of the replica
        self.largest_watermark[replica_idx] =
            Some(self.largest_watermark[replica_idx].map_or(ts, |w| w.max(ts)));

        // get new frontier
        match self.get_frontier() {
            Some(new_frontier) => match old_frontier {
                // if the old frontier is equal to the current one,
                // the watermark has already been sent
                Some(old_frontier) if new_frontier == old_frontier => None,
                // the frontier has been updated, send the watermark downstream
                _ => Some(new_frontier),
            },
            None => None,
        }
    }
}

#[derive(Debug, Derivative)]
#[derivative(Clone)]
pub struct StartBlock<Out: Data> {
    metadata: Option<ExecutionMetadata>,
    #[derivative(Clone(clone_with = "clone_empty"))]
    receivers: Vec<NetworkReceiver<NetworkMessage<Out>>>,
    buffer: VecDeque<StreamElement<Out>>,
    missing_ends: usize,
    missing_iter_ends: usize,
    prev_replicas: Vec<Coord>,
    /// The last call to next caused a timeout, so a FlushBatch has been emitted. In this case this
    /// field is set to true to avoid flooding with FlushBatch.
    already_timed_out: bool,
    /// The id of the previous blocks in the job graph.
    prev_block_ids: Vec<BlockId>,
    watermark_frontier: WatermarkFrontier,
}

impl<Out: Data> StartBlock<Out> {
    pub(crate) fn new(prev_block_id: BlockId) -> Self {
        Self {
            metadata: Default::default(),
            receivers: Default::default(),
            buffer: Default::default(),
            missing_ends: Default::default(),
            missing_iter_ends: Default::default(),
            prev_replicas: Default::default(),
            already_timed_out: Default::default(),
            prev_block_ids: vec![prev_block_id],
            watermark_frontier: Default::default(),
        }
    }

    pub(crate) fn concat(prev_block_ids: Vec<BlockId>) -> Self {
        Self {
            metadata: Default::default(),
            receivers: Default::default(),
            buffer: Default::default(),
            missing_ends: Default::default(),
            missing_iter_ends: Default::default(),
            prev_replicas: Default::default(),
            already_timed_out: Default::default(),
            prev_block_ids,
            watermark_frontier: Default::default(),
        }
    }

    /// The number of blocks that will send data to this block.
    ///
    /// This returns a meaningful value only after calling `setup`.
    pub(crate) fn num_prev(&self) -> usize {
        self.prev_replicas.len()
    }

    /// The list of previous blocks.
    pub(crate) fn prev(&self) -> &[BlockId] {
        &self.prev_block_ids
    }
}

impl<Out: Data> Operator<Out> for StartBlock<Out> {
    fn setup(&mut self, metadata: ExecutionMetadata) {
        let in_type = TypeId::of::<Out>();
        for &prev_block_id in &self.prev_block_ids {
            let mut network = metadata.network.lock().unwrap();
            let endpoint = ReceiverEndpoint::new(metadata.coord, prev_block_id);
            let receiver = network.get_receiver(endpoint);
            self.receivers.push(receiver);
            drop(network);
            for &(prev, typ) in metadata.prev.iter() {
                // ignore this connection because it refers to a different type, another StartBlock
                // in this block will handle it
                if in_type != typ {
                    continue;
                }
                if prev.block_id == prev_block_id {
                    self.prev_replicas.push(prev);
                }
            }
        }
        self.missing_ends = self.num_prev();
        self.missing_iter_ends = self.num_prev();
        self.watermark_frontier = WatermarkFrontier::new(self.prev_replicas.clone());

        info!(
            "StartBlock {} of {} initialized, {:?} are the previous blocks, a total of {} previous replicas",
            metadata.coord, std::any::type_name::<Out>(), self.prev_block_ids, self.num_prev()
        );
        self.metadata = Some(metadata);
    }

    fn next(&mut self) -> StreamElement<Out> {
        let metadata = self.metadata.as_ref().unwrap();
        // all the previous blocks sent an end: we're done
        if self.missing_ends == 0 {
            info!("StartBlock for {} has ended", metadata.coord);
            return StreamElement::End;
        }
        if self.missing_iter_ends == 0 {
            info!("StartBlock for {} has ended an iteration", metadata.coord);
            self.missing_iter_ends = self.num_prev();
            return StreamElement::IterEnd;
        }
        while self.buffer.is_empty() {
            let max_delay = metadata.batch_mode.max_delay();
            let buf = match (self.already_timed_out, max_delay) {
                // check the timeout only if there is one and the last time we didn't timed out
                (false, Some(max_delay)) => {
                    match NetworkReceiver::select_any_timeout(&self.receivers, max_delay) {
                        Ok(buf) => buf.result.expect("One of the receivers failed"),
                        Err(_) => {
                            // timed out: tell the block to flush the current batch
                            // next time we wait indefinitely without the timeout since the batch is
                            // currently empty
                            self.already_timed_out = true;
                            // this is a fake batch, and its sender is meaningless and will be
                            // forget immediately
                            NetworkMessage::new(vec![StreamElement::FlushBatch], Default::default())
                        }
                    }
                }
                _ => {
                    self.already_timed_out = false;
                    NetworkReceiver::select_any(&self.receivers)
                        .result
                        .expect("One of the receivers failed")
                }
            };

            let sender = buf.sender();
            let batch = buf.batch();

            // use watermarks to update the frontier
            let watermark_frontier = &mut self.watermark_frontier;
            self.buffer = batch
                .into_iter()
                .map(|item| match item {
                    StreamElement::Watermark(ts) => {
                        // update the frontier and return a watermark if necessary
                        watermark_frontier
                            .update(sender, ts)
                            .map(StreamElement::Watermark)
                    }
                    _ => Some(item),
                })
                .filter(|x| x.is_some())
                .map(Option::unwrap)
                .collect();
        }

        let message = self
            .buffer
            .pop_front()
            .expect("Previous block sent an empty message");
        if matches!(message, StreamElement::IterEnd) {
            self.missing_iter_ends -= 1;
            debug!(
                "{}/{:?} received an IterEnd, {} more to come",
                metadata.coord, self.prev_block_ids, self.missing_iter_ends
            );
            return self.next();
        }
        if matches!(message, StreamElement::End) {
            self.missing_ends -= 1;
            debug!(
                "{}/{:?} received an end, {} more to come",
                metadata.coord, self.prev_block_ids, self.missing_ends
            );
            return self.next();
        }
        message
    }

    fn to_string(&self) -> String {
        format!("[{}]", std::any::type_name::<Out>())
    }

    fn structure(&self) -> BlockStructure {
        let mut operator = OperatorStructure::new::<Out, _>("StartBlock");
        for &block_id in &self.prev_block_ids {
            operator
                .receivers
                .push(OperatorReceiver::new::<Out>(block_id));
        }
        BlockStructure::default().add_operator(operator)
    }
}

fn clone_empty<T>(_: &[T]) -> Vec<T> {
    Vec::new()
}
