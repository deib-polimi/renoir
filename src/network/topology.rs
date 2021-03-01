use std::collections::HashMap;
use std::marker::PhantomData;

use async_std::channel::bounded;
use typemap::{Key, ShareMap};

use crate::network::{Coord, NetworkReceiver, NetworkSender};

struct ReceiverKey<In>(PhantomData<In>);

impl<In> Key for ReceiverKey<In>
where
    In: 'static,
{
    type Value = HashMap<Coord, NetworkReceiver<In>>;
}

struct SenderKey<Out>(PhantomData<Out>);
impl<Out> Key for SenderKey<Out>
where
    Out: 'static,
{
    type Value = HashMap<Coord, NetworkSender<Out>>;
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
        T: Send + 'static,
    {
        let (sender, receiver) = bounded(1);
        let sender = NetworkSender::Local(sender);
        let receiver = NetworkReceiver::Local(receiver);
        self.senders
            .entry::<SenderKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, sender);
        self.receivers
            .entry::<ReceiverKey<T>>()
            .or_insert_with(|| Default::default())
            .insert(coord, receiver);
    }

    pub fn get_sender<T>(&self, coord: Coord) -> NetworkSender<T>
    where
        T: Send + 'static,
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

    pub fn get_receiver<T>(&mut self, coord: Coord) -> NetworkReceiver<T>
    where
        T: Send + 'static,
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
        for (coord, next) in self.next.iter() {
            topology += &format!("\n  {}:", coord);
            for next in next.iter() {
                topology += &format!(" {}", next);
            }
        }
        debug!("{}", topology);
    }
}
