extern crate csv;
extern crate timely;

#[cfg(test)]
use crate::dto::post::Post;
#[cfg(test)]
use crate::dto::common::Importable;
#[cfg(test)]
use timely::dataflow::InputHandle;
#[cfg(test)]
use timely::dataflow::channels::pact::Pipeline;

use crate::dto::common::{Timestamped, Watermarkable};

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub trait Buffer<G: Scope, P: ParallelizationContract<usize, D>, D: Data> {
    fn buffer(&self, pact: P) -> Stream<G, D>;
}

impl<
        G: Scope<Timestamp = usize>,
        P: ParallelizationContract<usize, D>,
        D: Data + Timestamped + Watermarkable,
    > Buffer<G, P, D> for Stream<G, D>
{
    fn buffer(&self, pact: P) -> Stream<G, D> {
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

#[test]
fn test_buffer_with_watermarks_emits_corret_number_of_events() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();

        let probe = worker.dataflow(|scope| {
            let input = scope.input_from(&mut input);

            input.buffer(Pipeline).inspect_batch(|t, xs| match t {
                2 => assert!(xs.len() == 2),
                6 => assert!(xs.len() == 3),
                _ => unreachable!(),
            });
            let probe = input.probe();

            probe
        });

        let input_data = vec![
            "1052740|332|2012-02-02T06:00:08Z|photo105274.jpg|14.102.224.16|Firefox||||154380|38",
            "1052750|332|2012-02-02T06:00:09Z|photo105275.jpg|14.102.224.16|Firefox||||154380|38",
            "Watermark|1052751",
            "1052760|332|2012-02-02T06:00:10Z|photo105276.jpg|14.102.224.16|Firefox||||154380|38",
            "1052770|332|2012-02-02T06:00:11Z|photo105277.jpg|14.102.224.16|Firefox||||154380|38",
            "1052770|332|2012-02-02T06:00:11Z|photo105277.jpg|14.102.224.16|Firefox||||154380|38",
            "Watermark|1052752",
            "1052780|332|2012-02-02T06:00:12Z|photo105278.jpg|62.114.0.3|Firefox||||154380|26",
        ];
        for round in 0..input_data.len() {
            let v: Vec<&str> = input_data[round].split("|").collect();
            let r = StringRecord::from(v);
            if &r[0] == "Watermark" {
                input.send(Post::from_watermark(&r[1]))
            } else {
                input.send(Post::from_record(r).unwrap())
            }

            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}
