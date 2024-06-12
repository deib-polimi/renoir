use serde::Serialize;
use std::fmt::Display;

use crate::{
    operator::{Operator, StreamElement},
    structure::{OperatorKind, OperatorStructure},
    ExecutionMetadata,
};

pub trait WriteOperator<T: Serialize>: Clone + Send {
    fn setup(&mut self, metadata: &ExecutionMetadata);
    fn write(&mut self, item: T);
    fn flush(&mut self);
    fn finalize(&mut self);
}

#[derive(Debug, Clone)]
pub struct WriterOperator<W, Op> {
    pub(super) prev: Op,
    pub(super) writer: W,
}

impl<W, Op: Operator> Display for WriterOperator<W, Op> {
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

impl<W, Op> Operator for WriterOperator<W, Op>
where
    Op: Operator,
    Op::Out: Serialize,
    W: WriteOperator<Op::Out>,
{
    type Out = ();

    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
        self.writer.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<()> {
        let el = self.prev.next();
        let ret = el.variant();
        match el {
            StreamElement::Item(item) | StreamElement::Timestamped(item, _) => {
                self.writer.write(item);
            }
            StreamElement::Watermark(_) => {}
            StreamElement::FlushBatch | StreamElement::FlushAndRestart => self.writer.flush(),
            StreamElement::Terminate => self.writer.finalize(),
        }
        ret
    }

    fn structure(&self) -> crate::structure::BlockStructure {
        let mut operator = OperatorStructure::new::<Op::Out, _>(format!(
            "WriterSink<{}>",
            std::any::type_name::<W>()
        ));
        operator.kind = OperatorKind::Sink;
        self.prev.structure().add_operator(operator)
    }
}
