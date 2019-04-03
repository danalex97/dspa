extern crate timely;

use std::collections::HashMap;
use std::cmp::min;

use crate::dto::post::Post;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::operators::source::KafkaSource;

use timely::dataflow::operators::{Inspect, FrontierNotificator};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;

const COLLECTION_PERIOD : usize = 1800; // seconds

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let mut comment_buffer: HashMap<usize, Vec<Comment>> = HashMap::new();

            scope
                .kafka_string_source::<Comment>("comments".to_string())
                .unary_notify(Pipeline, "CommentStats", None, move |input, output, notificator| {
                    let mut select_first = false;
                    let mut scheduled = false;
                    let mut vector = Vec::new();

                    input.for_each(|cap, comments| {
                        let time = cap.time().clone();

                        comments.swap(&mut vector);
                        for comment in vector.drain(..) {
                            comment_buffer
                                .entry(time)
                                .or_insert(vec![])
                                .push(comment);
                        }

                        // just schedule first point at the first received timestamp
                        if !scheduled {
                            scheduled = true;
                            notificator.notify_at(cap.delayed(&(time + COLLECTION_PERIOD)));
                        }
                    });

                    notificator.for_each(|cap, _, notificator| {
                        let time = cap.time().clone();

                        // process thumbling window
                        let mut session = output.session(&cap);
                        session.give(time);

                        notificator.notify_at(cap.delayed(&(time + COLLECTION_PERIOD)));
                    });
                })
                .inspect(|x| println!("{:?}", x));

            // scope.kafka_string_source::<Post>("posts".to_string())
            //     .inspect(|x| println!("{:?}", x));
        });
    }).unwrap();
}
