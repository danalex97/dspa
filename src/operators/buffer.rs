use crate::dto::common::Timestamped;
use std::collections::HashMap;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub trait Buffer<G: Scope, P: ParallelizationContract<usize, D>, D: Data> {
    fn buffer(&self, pact: P, delay: usize) -> Stream<G, D>;
}

impl<G: Scope<Timestamp = usize>, P: ParallelizationContract<usize, D>, D: Data + Timestamped>
    Buffer<G, P, D> for Stream<G, D>
{
    fn buffer(&self, pact: P, delay: usize) -> Stream<G, D> {
        let mut stash = HashMap::new();
        let mut epoch_start = 0;
        self.unary_notify(pact, "Buffer", None, move |input, output, notificator| {
            let mut vec = Vec::new();
            input.for_each(|cap, data| {
                data.swap(&mut vec);
                for datum in vec.drain(..) {
                    let time = datum.timestamp().clone();
                    if time > epoch_start + delay {
                        notificator.notify_at(cap.delayed(&(time + delay)));
                        epoch_start = time;
                    }
                    stash.entry(epoch_start).or_insert(vec![]).push(datum);
                }
            });

            notificator.for_each(|cap, _, _| {
                if let Some(mut vec) = stash.remove(&(cap.time() - delay)) {
                    output.session(&cap).give_iterator(vec.drain(..));
                }
            })
        })
    }
}
