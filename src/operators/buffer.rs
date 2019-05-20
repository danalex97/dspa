use crate::dto::common::{Timestamped, Watermarkable};
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub trait Buffer<G: Scope, P: ParallelizationContract<usize, D>, D: Data> {
    fn buffer(&self, pact: P, delay: usize) -> Stream<G, D>;
}

impl<
        G: Scope<Timestamp = usize>,
        P: ParallelizationContract<usize, D>,
        D: Data + Timestamped + Watermarkable,
    > Buffer<G, P, D> for Stream<G, D>
{
    fn buffer(&self, pact: P, _delay: usize) -> Stream<G, D> {
        let mut data_stash = vec![];
        self.unary(pact, "Buffer", move |_, _| {
            move |input, output| {
                let mut vector = vec![];
                while let Some((time, data)) = input.next() {
                    data.swap(&mut vector);
                    let mut needs_release = false;
                    for d in vector.drain(..) {
                        if d.is_watermark() {
                            needs_release = true;
                        } else {
                            data_stash.push((d.timestamp().clone(), d));
                        }
                    }
                    if needs_release {
                        // Release everything up to watermark
                        let mut session = output.session(&time);
                        data_stash.sort_by(|(t1, _), (t2, _)| t1.partial_cmp(t2).unwrap());
                        for (_post_time, post) in data_stash.drain(..) {
                            session.give(post.clone());
                        }
                    }
                }
            }
        })
    }
}
