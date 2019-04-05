extern crate timely;

use std::collections::HashMap;
use std::cmp::min;

use crate::dto::post::Post;
use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::operators::source::KafkaSource;
use crate::connection::producer::FIXED_BOUNDED_DELAY;

use timely::dataflow::operators::{Inspect, FrontierNotificator};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;
use self::timely::dataflow::channels::pact::Exchange;
use crate::dto::like::Like;
use crate::operators::buffer::Buffer;
use self::timely::dataflow::ProbeHandle;
use self::timely::dataflow::operators::probe::Probe;
use self::timely::dataflow::operators::input::Input;

const COLLECTION_PERIOD : usize = 1800; // seconds

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let comments = scope.kafka_string_source::<Comment>("comments".to_string());
            let likes = scope.kafka_string_source::<Like>("likes".to_string());

            let mut count_likes = HashMap::new();
            let mut epoch_start = 0;
            let counted_likes = likes.buffer(Exchange::new(|l: &Like| l.post_id as u64), FIXED_BOUNDED_DELAY)
                .unary_notify(Pipeline, "CountLikes", None, move |input, output, notificator| {
                    let mut vec = vec![];
                    input.for_each(|cap, data| {
                        data.swap(&mut vec);
                        for datum in vec.drain(..) {
                            let time = datum.timestamp().clone();
                            if time > epoch_start + FIXED_BOUNDED_DELAY {
                                notificator.notify_at(cap.delayed(&(time + FIXED_BOUNDED_DELAY)));
                                epoch_start = time;
                            }
                            let post_entry = count_likes.entry(epoch_start).or_insert(HashMap::new());
                            post_entry.entry(datum.post_id)
                                .and_modify(|e| { *e += 1 })
                                .or_insert(1);
                        }
                    });

                    notificator.for_each(|cap, _, _| {
                        if let Some(mut map) = count_likes.remove(&(cap.time() - FIXED_BOUNDED_DELAY)) {
                            output.session(&cap).give_iterator(map.drain());
                        }
                    });
                });

            let mut all_posts = HashMap::new();
            let mut output_epoch_start = 0;
            let mut output_epoch_start_times = vec![];
            let mut posts_buffer = HashMap::new();
            let mut likes_count_buffer = HashMap::new();

            posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY)
                .binary_notify(&counted_likes, Pipeline, Pipeline, "AggregateLikes", None, move |p_input, l_input, output, notificator| {
                    let mut p_data = Vec::new();
                    p_input.for_each(|cap, input| {
                        if output_epoch_start == 0 {
                            // First post received
                            output_epoch_start = cap.time().clone();
                            output_epoch_start_times.push(output_epoch_start);
                            notificator.notify_at(cap.delayed(&(output_epoch_start + COLLECTION_PERIOD)));
                        }
                        input.swap(&mut p_data);
                        for post in p_data.drain(..) {
                            //let time = post.timestamp().clone();
                            let time = cap.time().clone();
                            if time > output_epoch_start + COLLECTION_PERIOD {
                                while time > output_epoch_start + COLLECTION_PERIOD {
                                    // Ensure we have exact 30 min boundaries
                                    output_epoch_start += COLLECTION_PERIOD;
                                    output_epoch_start_times.push(output_epoch_start);
                                }
                                notificator.notify_at(cap.delayed(&(output_epoch_start + COLLECTION_PERIOD)));
                            }
                            posts_buffer.entry(output_epoch_start).or_insert(vec![]).push(post);
                        }
                    });

                    let mut l_data = Vec::new();
                    l_input.for_each(|cap, input| {
                       input.swap(&mut l_data);
                        for (post, count) in l_data.drain(..) {
                            let time = cap.time().clone();
                            for epoch_start in output_epoch_start_times.iter() {
                                if epoch_start + COLLECTION_PERIOD > time && time > *epoch_start {
                                    let post_entry = likes_count_buffer.entry(epoch_start.clone()).or_insert(HashMap::new());
                                    post_entry.entry(post)
                                        .and_modify(|e| { *e += count })
                                        .or_insert(count);
                                    break;
                                }
                            }
                        }
                    });

                    notificator.for_each(|cap, count, notificator| {
                        if let Some(mut new_posts) = posts_buffer.remove(&(cap.time() - COLLECTION_PERIOD)) {
                            for post in new_posts.drain(..) {
                                all_posts.insert(post.id, post);
                            }
                        }
                        if let Some(mut post_likes) = likes_count_buffer.remove(&(cap.time() - COLLECTION_PERIOD)) {
                            for (post, likes) in post_likes.drain() {
                                match all_posts.get_mut(&post) {
                                    None => {/* no-op, probably because data is ordered poorly */},
                                    Some(post) => post.likes += likes,
                                }
                            }
                        }
                        let counts: Vec<(u32, u32)> = all_posts.iter().map(|(id, post)| {
                            (id.clone(), post.likes)
                        }).collect();
                        output.session(&cap).give(counts);
                    });


                })
                .inspect_batch(|t, xs| println!("@{}: {:?}", t, xs));
        });
    }).unwrap();
}
