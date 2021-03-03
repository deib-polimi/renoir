use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

use async_std::channel::bounded;
use itertools::Itertools;
use typemap::{Key, ShareMap};

use crate::network::{Coord, NetworkMessage, NetworkReceiver, NetworkSender};

struct ReceiverKey<In>(PhantomData<In>);

impl<In> Key for ReceiverKey<In>
where
    In: Clone + Send + 'static,
{
    type Value = HashMap<Coord, NetworkReceiver<NetworkMessage<In>>>;
}

struct SenderKey<Out>(PhantomData<Out>);
impl<Out> Key for SenderKey<Out>
where
    Out: Clone + Send + 'static,
{
    type Value = HashMap<Coord, NetworkSender<NetworkMessage<Out>>>;
}

pub struct NetworkTopology {
    receivers: ShareMap,
    senders: ShareMap,
    next: HashMap<Coord, Vec<Coord>>,
}

impl NetworkTopology {
    pub fn new() -> Self {
        NetworkTopology {
            receivers: ShareMap::custom(),
            senders: ShareMap::custom(),
            next: Default::default(),
        }
    }

    pub fn register_local<T>(&mut self, coord: Coord)
    where
        T: Clone + Send + 'static,
    {
        let (sender, receiver) = bounded(1);
        let sender = NetworkSender::local(coord, sender);
        let receiver = NetworkReceiver::local(coord, receiver);
        self.senders
            .entry::<SenderKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, sender);
        self.receivers
            .entry::<ReceiverKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, receiver);
    }

    pub fn get_sender<T>(&self, coord: Coord) -> NetworkSender<NetworkMessage<T>>
    where
        T: Clone + Send + 'static,
    {
        let map = self.senders.get::<SenderKey<T>>().unwrap_or_else(|| {
            panic!(
                "No sender registered for type: {}",
                std::any::type_name::<T>()
            )
        });
        map.get(&coord)
            .unwrap_or_else(|| {
                panic!(
                    "Sender for ({}, {:?}) not registered",
                    std::any::type_name::<T>(),
                    coord
                )
            })
            .clone()
    }

    pub fn get_senders<T>(&self, coord: Coord) -> HashMap<Coord, NetworkSender<NetworkMessage<T>>>
    where
        T: Clone + Send + 'static,
    {
        match self.next.get(&coord) {
            None => Default::default(),
            Some(next) => next.iter().map(|&c| (c, self.get_sender(c))).collect(),
        }
    }

    pub fn get_receiver<T>(&mut self, coord: Coord) -> NetworkReceiver<NetworkMessage<T>>
    where
        T: Clone + Send + 'static,
    {
        let map = self
            .receivers
            .get_mut::<ReceiverKey<T>>()
            .unwrap_or_else(|| {
                panic!(
                    "No receiver registered for type: {}",
                    std::any::type_name::<T>()
                )
            });
        map.remove(&coord).unwrap_or_else(|| {
            panic!(
                "Sender for ({}, {:?}) not registered",
                std::any::type_name::<T>(),
                coord
            )
        })
    }

    pub fn connect(&mut self, from: Coord, to: Coord) {
        self.next.entry(from).or_default().push(to);
    }

    pub fn log_topology(&self) {
        let mut topology = "Execution graph:".to_owned();
        for (coord, next) in self.next.iter().sorted() {
            topology += &format!("\n  {}:", coord);
            for next in next.iter().sorted() {
                topology += &format!(" {}", next);
            }
        }
        debug!("{}", topology);
    }
}

impl Debug for NetworkTopology {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkTopology")
            .field("num_receivers", &self.receivers.len())
            .field("num_senders", &self.senders.len())
            .field("next", &self.next)
            .finish()
    }
}
