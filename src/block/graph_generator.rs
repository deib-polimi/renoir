use std::collections::HashMap;

use itertools::Itertools;

use crate::block::{BlockStructure, ConnectionStrategy, DataType, OperatorKind};
use crate::stream::BlockId;

/// This struct is able to track the block structure of all the blocks of the job graph for later
/// producing a diagram in dot format.
#[derive(Clone, Debug)]
pub struct JobGraphGenerator {
    /// The list of known blocks, indexed by block id.
    blocks: HashMap<BlockId, BlockStructure>,
}

impl JobGraphGenerator {
    pub fn new() -> Self {
        Self {
            blocks: Default::default(),
        }
    }

    /// Register a new block inside the generator.
    ///
    /// If a block with the same id has already been registered, the structure will be overwritten.
    pub fn add_block(&mut self, block_id: BlockId, structure: BlockStructure) {
        self.blocks.insert(block_id, structure);
    }

    /// Finalize the generator and generate a string representation of the job graph in dot format.
    pub fn finalize(self) -> String {
        let attributes = vec!["ranksep=0.1"];
        format!(
            "digraph rstream2 {{\n{attributes}\n{subgraphs}\n{connections}\n}}",
            attributes = attributes
                .into_iter()
                .map(|s| format!("  {};", s))
                .join("\n"),
            subgraphs = self.gen_subgraphs(),
            connections = self.gen_connections()
        )
    }

    /// Each block will have its own `subgraph`, this function will generate the `subgraph`s for all
    /// the blocks in the network.
    fn gen_subgraphs(&self) -> String {
        let mut result = String::new();
        for &block_id in self.blocks.keys().sorted() {
            let block = self.blocks.get(&block_id).unwrap();
            result += &self.gen_subgraph(block_id, block);
        }

        result
    }

    /// Generate the `subgraph` for a specific block.
    ///
    /// This will generate all the nodes and attributes, as well as all the connection from an
    /// operator to the next inside the block.
    fn gen_subgraph(&self, block_id: BlockId, block: &BlockStructure) -> String {
        let cluster_id = format!("cluster_block{}", block_id);
        let attributes = vec![
            "style=filled".to_string(),
            "color=lightgrey".to_string(),
            "labeljust=l".to_string(),
            "edge[fontname=\"monospace\"]".to_string(),
            format!("label=\"Block {}\"", block_id),
        ];
        let mut nodes = vec![];
        let mut connections = vec![];

        for (index, operator) in block.operators.iter().enumerate() {
            let id = Self::operator_id(block_id, index);
            let label = &operator.title; // TODO: escape
            let shape = match operator.kind {
                OperatorKind::Operator => "box",
                OperatorKind::Sink => "house",
                OperatorKind::Source => "invhouse",
            };
            let typ = &operator.out_type;
            nodes.push(format!("{} [label=\"{}\",shape={}]", id, label, shape));
            if index < block.operators.len() - 1 {
                let next = Self::operator_id(block_id, index + 1);
                connections.push(format!(
                    "{} -> {} [label=\"    {}\",labeljust=l,labelfloat=true]",
                    id, next, typ
                ));
            }
        }

        let attributes = attributes
            .into_iter()
            .map(|s| format!("    {};", s))
            .join("\n");
        let nodes = nodes.into_iter().map(|s| format!("    {};", s)).join("\n");
        let connections = connections
            .into_iter()
            .map(|s| format!("    {};", s))
            .join("\n");

        format!(
            "  subgraph {} {{\n{}\n{}\n{}\n  }}\n",
            cluster_id, attributes, nodes, connections
        )
    }

    /// Generate the connections between the operators in different blocks,
    fn gen_connections(&self) -> String {
        let mut receivers: HashMap<(BlockId, BlockId), (usize, DataType)> = Default::default();
        for (&block_id, block) in &self.blocks {
            for (index, operator) in block.operators.iter().enumerate() {
                for receiver in &operator.receivers {
                    receivers.insert(
                        (receiver.previous_block_id, block_id),
                        (index, receiver.data_type.clone()),
                    );
                }
            }
        }

        let mut result = vec![];
        for (&from_block, block) in &self.blocks {
            for (from_index, operator) in block.operators.iter().enumerate() {
                for connection in &operator.connections {
                    let to_block = connection.to_block_id;
                    let (to_index, data_type) = if let Some((to_index, data_type)) =
                        receivers.get(&(from_block, to_block))
                    {
                        (*to_index, data_type)
                    } else {
                        (0, &connection.data_type)
                    };
                    let style = match connection.strategy {
                        ConnectionStrategy::OnlyOne => "dotted",
                        ConnectionStrategy::Random => "solid",
                        ConnectionStrategy::GroupBy => "dashed",
                    };
                    let sublabel = match connection.strategy {
                        ConnectionStrategy::OnlyOne => "only-one",
                        ConnectionStrategy::Random => "shuffle",
                        ConnectionStrategy::GroupBy => "group-by",
                    };

                    let from_id = Self::operator_id(from_block, from_index);
                    let to_id = Self::operator_id(to_block, to_index);
                    result.push(format!(
                        "{} -> {} [label=\"{}\\n{}\",labelfloat=true,style={}]",
                        from_id, to_id, data_type, sublabel, style
                    ));
                }
            }
        }
        result.into_iter().map(|s| format!("  {};", s)).join("\n")
    }

    /// Return the identifier of an operator.
    fn operator_id(block_id: BlockId, index: usize) -> String {
        format!("block{}_operator{}", block_id, index)
    }
}
