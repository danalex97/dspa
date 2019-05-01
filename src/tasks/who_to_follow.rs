extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::operators::source::KafkaSource;

use crate::operators::active_posts::ActivePosts;
use crate::operators::link_replies::LinkReplies;

use crate::dto::comment::Comment;
use crate::dto::common::Timestamped;
use crate::dto::like::Like;
use crate::dto::post::Post;

use crate::dsa::stash::*;
use crate::dto::parse::*;
use crate::dto::forum::Forum;
use crate::connection::import::csv_to_map;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::Inspect;

use std::collections::HashMap;
use std::collections::HashSet;

const FORUM_PATH: &str = "data/1k-users-sorted/tables/forum.csv";
const FORUM_MEMEBERS_PATH: &str = "data/1k-users-sorted/tables/forum_hasMember_person.csv";

const COLLECTION_PERIOD: usize = 60 * 60; // seconds
const ACTIVE_POST_PERIOD: usize = 4 * 60 * 60; // seconds

pub fn run() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string());
            let comments = scope.kafka_string_source::<Comment>("comments".to_string());
            let likes = scope.kafka_string_source::<Like>("likes".to_string());

            let buffered_likes = likes.buffer(
                Exchange::new(|l: &Like| l.post_id as u64),
                FIXED_BOUNDED_DELAY,
            );

            let buffered_posts =
                posts.buffer(Exchange::new(|p: &Post| p.id as u64), FIXED_BOUNDED_DELAY);

            let linked_comments = comments.broadcast().link_replies(
                &buffered_posts,
                Pipeline,
                Pipeline,
                FIXED_BOUNDED_DELAY,
            );

            let active_posts = linked_comments.active_post_ids(
                &buffered_likes,
                Pipeline,
                Pipeline,
                FIXED_BOUNDED_DELAY,
                ACTIVE_POST_PERIOD,
            );

            let people_of_interest: Vec<u32> = vec![129, 986, 618, 296, 814, 379, 441, 655, 836, 929];

            // getting the forum map
            let mut forum_map = csv_to_map::<Forum>(FORUM_PATH);
            parse_forum_member_csv(FORUM_MEMEBERS_PATH, &mut forum_map);

            let mut first_notified = false;
            let mut post_info = HashMap::new(); // map: post_id -> (forum, tags)
            // let mut people_engaged_with_forum = HashMap::new(); // map: forum -> set[people]
            // let mut people_engaged_with_tag = HashMap::new(); // map: tag -> set[people]
            // vector of (post_id, interacting_users)
            let mut active_post_snapshot = Vec::new();
            active_posts
                .binary_notify(
                    &buffered_posts,
                    Pipeline,
                    Pipeline,
                    "WhoToFollow",
                    None,
                    move |ap_input, bp_input, output, notificator| {
                        // keep all bp information from beginning of time
                        let mut bp_data = Vec::new();
                        bp_input.for_each(|cap, input| {
                            input.swap(&mut bp_data);
                            for post in bp_data.drain(..) {
                                post_info
                                    .entry(post.id)
                                    .or_insert((post.forum_id, post.tags));
                            }
                            if !first_notified {
                                notificator.notify_at(cap.delayed(
                                    &(cap.time() + COLLECTION_PERIOD - FIXED_BOUNDED_DELAY),
                                ));
                                first_notified = true;
                            }
                        });

                        // keep the latest snapshot that we received
                        ap_input.for_each(|cap, input| {
                            input.swap(&mut active_post_snapshot);
                        });

                        // do the actual computation at each notification
                        notificator.for_each(|cap, _, notificator| {
                            notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));

                            // the posts that our users are engaged with
                            let mut posts_of_interest = HashMap::new(); // map: user -> set[posts]
                            // you have the guarantee that we will have a new snapshot before the
                            // next notification, so we can drain it
                            for (post_id, engaged_people) in active_post_snapshot.drain(..) {
                                // println!("{:?} {:?}", post_id, engaged_people);
                                for person_id in engaged_people {
                                    if people_of_interest.contains(&person_id) {
                                        posts_of_interest
                                            .entry(person_id)
                                            .or_insert(HashSet::new())
                                            .insert(post_id);
                                    }
                                }
                            }

                            for (interest_person_id, post_set) in posts_of_interest {
                                // find all the unique forums this person is interested in
                                let mut forum_set = HashSet::new();
                                for post_id in post_set {
                                    let (forum, _) = post_info.get(&post_id).unwrap();
                                    forum_set.insert(forum);
                                }

                                let mut candidate_friends = HashSet::new();
                                for forum_id in forum_set {
                                    if let Some(forum) = forum_map.get(forum_id) {
                                        for forum_member in forum.member_ids.clone() {
                                            candidate_friends.insert(forum_member);
                                        }
                                    }
                                }

                                // [TODO] 
                            }

                            let mut session = output.session(&cap);
                            session.give(1);
                        })
                    }
                );
                // .inspect_batch(|t, xs| println!("@t {:?}: {:?}", t, xs));
        });
    })
        .unwrap();
}
