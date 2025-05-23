use serde::Serialize;
use std::{ffi::OsString, fmt::Display, path::PathBuf};

use crate::{
    block::structure::{OperatorKind, OperatorStructure},
    operator::{Operator, StreamElement},
    ExecutionMetadata,
};

pub trait WriteOperator<T: Serialize>: Clone + Send {
    type Destination;
    fn setup(&mut self, destination: Self::Destination);
    fn write(&mut self, items: &mut impl Iterator<Item = T>);
    fn flush(&mut self);
    fn finalize(&mut self);
}

#[derive(Debug, Clone)]
pub struct WriterOperator<Op, W, D> {
    prev: Op,
    writer: W,
    make_destination: Option<D>,
}

impl<Op, W, D> WriterOperator<Op, W, D>
where
    Op: Operator,
    Op::Out: Serialize,
    W: WriteOperator<Op::Out>,
    D: FnOnce(&ExecutionMetadata) -> W::Destination + Send + Clone,
{
    pub fn new(prev: Op, writer: W, make_destination: D) -> Self {
        Self {
            prev,
            writer,
            make_destination: Some(make_destination),
        }
    }
}

impl<W, Op: Operator, D> Display for WriterOperator<Op, W, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> WriterSink<{}, {}>",
            self.prev,
            std::any::type_name::<W>(),
            std::any::type_name::<Op::Out>()
        )
    }
}

impl<Op, W, D> Operator for WriterOperator<Op, W, D>
where
    Op: Operator,
    Op::Out: Serialize,
    W: WriteOperator<Op::Out>,
    D: FnOnce(&ExecutionMetadata) -> W::Destination + Send + Clone,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.writer
            .setup((self.make_destination.take().unwrap())(metadata));
    }

    fn next(&mut self) -> StreamElement<()> {
        struct SequenceIterator<'a, Op: Operator> {
            prev: &'a mut Op,
            last: Option<StreamElement<Op::Out>>,
        }

        impl<Op: Operator> Iterator for SequenceIterator<'_, Op> {
            type Item = Op::Out;

            fn next(&mut self) -> Option<Self::Item> {
                match self.prev.next() {
                    StreamElement::Item(item) | StreamElement::Timestamped(item, _) => Some(item),
                    other => {
                        self.last = Some(other);
                        None
                    }
                }
            }
        }

        let mut s = SequenceIterator {
            prev: &mut self.prev,
            last: None,
        };
        self.writer.write(&mut s);
        let result = s.last.take().unwrap().variant();
        match &result {
            StreamElement::FlushBatch | StreamElement::FlushAndRestart => self.writer.flush(),
            StreamElement::Terminate => self.writer.finalize(),
            _ => {}
        }
        result
    }

    fn structure(&self) -> crate::block::structure::BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>(format!(
            "WriterSink<{}>",
            std::any::type_name::<W>()
        ));
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}

pub fn sequential_path(base: PathBuf, metadata: &ExecutionMetadata) -> PathBuf {
    let mut path = base;
    let id = metadata.global_id;
    if path.is_dir() {
        path.push(format!("{id:04}"));
    } else {
        let file_name = path.file_stem().unwrap_or_default();
        let ext = path.extension();
        let mut name = OsString::new();
        name.push(file_name);
        name.push(format!("{id:04}"));
        if let Some(ext) = ext {
            name.push(".");
            name.push(ext);
        }
        path.pop();
        path.push(name);
    }
    path
}
