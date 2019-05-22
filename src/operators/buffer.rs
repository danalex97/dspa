extern crate csv;
extern crate timely;

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
                        }
                        // Even the watermark will propagate so that we report data all the time
                        // for the first 2 tasks
                        data_stash.push((d.timestamp().clone(), d));
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

#[cfg(test)]
#[rustfmt::skip]
mod buffers_tests {
    extern crate timely;

    use crate::dto::common::Watermarkable;
    use crate::dto::post::Post;

    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::InputHandle;
    use timely::dataflow::operators::{Input, Inspect, Probe};

    use crate::operators::buffer::Buffer;

    #[test]
    fn test_buffer_emits_correct_batches() {
        timely::execute_from_args(std::env::args(), |worker| {
            let mut posts_input = InputHandle::new();

            let default_post = Post{is_watermark:false, ..Post::from_watermark("0")};
            let posts_data = vec![
                Post{id:1, timestamp:1, ..default_post.clone()},
                Post{id:2, timestamp:7, ..default_post.clone()},
                Post{id:3, timestamp:3, ..default_post.clone()},
                Post::from_watermark("10"),
                Post{id:4, timestamp:11, ..default_post.clone()},
                Post{id:5, timestamp:13, ..default_post.clone()},
                Post{id:6, timestamp:12, ..default_post.clone()},
                Post::from_watermark("15"),
            ];

            let probe = worker.dataflow(|scope| {
                let posts = scope.input_from(&mut posts_input);
                posts.buffer(Pipeline).inspect_batch(|t, xs: &[Post]| match t {
                    8  => assert_eq!(xs.iter().map(|x| x.timestamp).collect::<Vec<_>>(), vec![1,3,7,10]),
                    13 => assert_eq!(xs.iter().map(|x| x.timestamp).collect::<Vec<_>>(), vec![11,12,13,15]),
                    _  => unreachable!(),
                });
                posts.probe()
            });

            let post_batches = vec![
                (8, posts_data[0..2].to_vec()),
                (10, posts_data[2..4].to_vec()),
                (13, posts_data[4..7].to_vec()),
                (15, posts_data[7..8].to_vec()),
            ];
            for (t, mut data) in post_batches {
                posts_input.send_batch(&mut data);
                posts_input.advance_to(t);
                while probe.less_than(posts_input.time()) {
                     worker.step();
                }
            }
        })
        .unwrap();
    }
}
