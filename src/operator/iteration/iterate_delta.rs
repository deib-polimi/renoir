use serde::{Deserialize, Serialize};

use crate::block::{BlockStructure, OperatorStructure};
use crate::operator::iteration::IterationStateHandle;
use crate::operator::{
    ExchangeData, ExchangeDataKey, Operator, SingleStartBlockReceiverOperator, StreamElement,
};
use crate::scheduler::ExecutionMetadata;
use crate::KeyedStream;

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
struct TerminationCond {
    something_changed: bool,
    last_iteration: bool,
    iter: usize,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
enum Msg<I, U, D, O> {
    Init(I),
    Update(U),
    Delta(D),
    Output(O),
}

impl<I, U, D, O> Msg<I, U, D, O> {
    /// Returns `true` if the msg is [`Update`].
    ///
    /// [`Update`]: Msg::Update
    #[must_use]
    fn is_update(&self) -> bool {
        matches!(self, Self::Update(..))
    }

    /// Returns `true` if the msg is [`Output`].
    ///
    /// [`Output`]: Msg::Output
    #[must_use]
    fn is_output(&self) -> bool {
        matches!(self, Self::Output(..))
    }

    fn unwrap_update(self) -> U {
        if let Self::Update(v) = self {
            v
        } else {
            panic!("unwrap on wrong iteration message type")
        }
    }

    fn unwrap_output(self) -> O {
        if let Self::Output(v) = self {
            v
        } else {
            panic!("unwrap on wrong iteration message type")
        }
    }
}

#[derive(Clone)]
pub struct DeltaIterate<
    Key: ExchangeData,
    I: ExchangeData,
    U: ExchangeData,
    D: ExchangeData,
    O: ExchangeData,
> {
    prev: SingleStartBlockReceiverOperator<(Key, Msg<I, U, D, O>)>,
}

impl<Key: ExchangeData, I: ExchangeData, U: ExchangeData, D: ExchangeData, O: ExchangeData>
    Operator<(Key, U)> for DeltaIterate<Key, I, U, D, O>
{
    fn setup(&mut self, metadata: &mut ExecutionMetadata) {
        self.prev.setup(metadata);
    }

    fn next(&mut self) -> StreamElement<(Key, U)> {
        self.prev.next().map(|(k, v)| (k, v.unwrap_update()))
    }

    fn structure(&self) -> BlockStructure {
        self.prev
            .structure()
            .add_operator(OperatorStructure::new::<(Key, U), _>("DeltaIterate"))
    }
}

impl<Key: ExchangeData, I: ExchangeData, U: ExchangeData, D: ExchangeData, O: ExchangeData>
    std::fmt::Display for DeltaIterate<Key, I, U, D, O>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UpdateStart")
    }
}

impl<Key: ExchangeDataKey, In: ExchangeData + Default, OperatorChain>
    KeyedStream<Key, In, OperatorChain>
where
    OperatorChain: Operator<(Key, In)> + 'static,
{
    /// TODO DOCS
    pub fn delta_iterate<U: ExchangeData, D: ExchangeData, O: ExchangeData, Body, BodyOperator>(
        self,
        num_iterations: usize,
        process_delta: impl Fn(&Key, &mut In, D) + Clone + Send + 'static,
        make_update: impl Fn(&Key, &mut In) -> U + Clone + Send + 'static,
        make_output: impl Fn(&Key, In) -> O + Clone + Send + 'static,
        condition: impl Fn(&D) -> bool + Clone + Send + 'static,
        body: Body,
    ) -> KeyedStream<Key, O, impl Operator<(Key, O)>>
    where
        Body: FnOnce(
                KeyedStream<Key, U, DeltaIterate<Key, In, U, D, O>>,
            ) -> KeyedStream<Key, D, BodyOperator>
            + 'static,
        BodyOperator: Operator<(Key, D)> + 'static,
    {
        let (state, out) = self.map(|(_, v)| Msg::Init(v)).unkey().iterate(
            num_iterations,
            TerminationCond {
                something_changed: false,
                last_iteration: false,
                iter: 0,
            },
            move |s, state: IterationStateHandle<TerminationCond>| {
                let mut routes = s
                    .to_keyed()
                    .rich_map({
                        let mut local_state: In = Default::default();
                        move |(k, msg): (_, Msg<_, _, _, _>)| {
                            let state = state.get();
                            if state.last_iteration || state.iter == num_iterations - 2 {
                                return Msg::Output(make_output(
                                    k,
                                    std::mem::take(&mut local_state),
                                ));
                            }

                            match msg {
                                Msg::Init(init) => local_state = init,
                                Msg::Delta(delta) => process_delta(k, &mut local_state, delta),
                                _ => unreachable!("invalid message at DeltaIterate start"),
                            }

                            Msg::Update(make_update(k, &mut local_state))
                        }
                    })
                    .unkey()
                    .route()
                    .add_route(|(_, v)| v.is_update())
                    .add_route(|(_, v)| v.is_output())
                    .build_inner()
                    .into_iter();

                let update_stream = body(
                    routes
                        .next()
                        .unwrap()
                        .to_keyed()
                        .add_operator(|prev| DeltaIterate { prev }),
                )
                .map(|(_, v)| Msg::Delta(v))
                .unkey();
                let output_stream = routes.next().unwrap();

                update_stream.concat(output_stream)
            },
            move |changed: &mut TerminationCond, x| match x.1 {
                Msg::Delta(u) if (condition)(&u) => changed.something_changed = true,
                Msg::Delta(_) => {}
                Msg::Output(_) => changed.last_iteration = true,
                _ => unreachable!(),
            },
            |global, local| {
                global.something_changed |= local.something_changed;
                global.last_iteration |= local.last_iteration;
            },
            |s| {
                let cond = !s.last_iteration;
                if !s.something_changed {
                    s.last_iteration = true;
                }
                s.something_changed = false;
                s.iter += 1;
                cond
            },
        );

        state.for_each(std::mem::drop);
        out.to_keyed().map(|(_, v)| v.unwrap_output())
    }
}
