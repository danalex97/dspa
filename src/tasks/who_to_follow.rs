extern crate timely;

use crate::connection::producer::FIXED_BOUNDED_DELAY;
use crate::operators::buffer::Buffer;
use crate::operators::source::KafkaSource;

use crate::operators::active_posts::ActivePosts;
use crate::operators::link_replies::LinkReplies;

use crate::dto::comment::Comment;
use crate::dto::like::Like;
use crate::dto::post::Post;

use crate::connection::import::csv_to_map;
use crate::dto::forum::Forum;
use crate::dto::parse::*;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::Inspect;

use crate::dto::person::Person;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hasher;
use std::iter::FromIterator;
use std::path::PathBuf;
use timely::Configuration;

const COLLECTION_PERIOD: usize = 60 * 60; // seconds
const ACTIVE_POST_PERIOD: usize = 4 * 60 * 60; // seconds
const RECOMMENDATIONS: usize = 5;

pub fn run(tables_path: PathBuf) {
    timely::execute(Configuration::Process(4), move |worker| {
        let index = worker.index();
        worker.dataflow::<usize, _, _>(|scope| {
            let posts = scope.kafka_string_source::<Post>("posts".to_string(), index);
            let comments = scope.kafka_string_source::<Comment>("comments".to_string(), index);
            let likes = scope.kafka_string_source::<Like>("likes".to_string(), index);

            let buffered_likes = likes.buffer(Exchange::new(|l: &Like| {
                if l.is_watermark {
                    return l.post_id as u64;
                }
                let mut hasher = DefaultHasher::new();
                hasher.write_u32(l.post_id);
                hasher.finish()
            }));
            let buffered_posts = posts.buffer(Exchange::new(|p: &Post| {
                if p.is_watermark {
                    return p.id as u64;
                }
                let mut hasher = DefaultHasher::new();
                hasher.write_u32(p.id);
                hasher.finish()
            }));

            let linked_comments = comments.broadcast().buffer(Pipeline).link_replies(
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

            let people_of_interest: Vec<u32> =
                vec![129, 986, 618, 296, 814, 379, 441, 655, 836, 929];

            // getting the forum map
            let forum_path = tables_path.join("forum.csv");
            let mut forum_map = csv_to_map::<Forum>(forum_path.to_str().unwrap());

            let forum_members_path = tables_path.join("forum_hasMember_person.csv");
            parse_forum_member_csv(forum_members_path.to_str().unwrap(), &mut forum_map);

            let person_path = tables_path.join("person.csv");
            let mut person_map = csv_to_map::<Person>(person_path.to_str().unwrap());

            let person_friend_path = tables_path.join("person_knows_person.csv");
            parse_person_friends(person_friend_path.to_str().unwrap(), &mut person_map);

            let mut person_forums: HashMap<u32, HashSet<u32>> = HashMap::new();
            for (forum_id, forum) in &forum_map {
                for member in &forum.member_ids {
                    person_forums
                        .entry(*member)
                        .or_insert(HashSet::new())
                        .insert(*forum_id);
                }
            }

            let mut first_notified = false;
            let mut post_info = HashMap::new(); // map: post_id -> (forum, tags)
            let mut active_posts_at_time: HashMap<usize, Vec<(u32, HashSet<u32>)>> = HashMap::new();
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
                                notificator
                                    .notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));
                                first_notified = true;
                            }
                        });

                        // keep the latest snapshot that we received
                        ap_input.for_each(|cap, input| {
                            let mut vec = vec![];
                            input.swap(&mut vec);
                            active_posts_at_time.insert(*cap.time(), vec);
                        });

                        // do the actual computation at each notification
                        notificator.for_each(|cap, _, notificator| {
                            notificator.notify_at(cap.delayed(&(cap.time() + COLLECTION_PERIOD)));

                            // the posts that our users are engaged with
                            let mut posts_of_interest = HashMap::new(); // map: user -> set[posts]

                            // you have the guarantee that we will have a new snapshot before the
                            // next notification, so we can drain it
                            for (post_id, engaged_people) in active_posts_at_time
                                .remove(cap.time())
                                .unwrap_or(vec![])
                                .drain(..)
                            {
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

                                // Filter out friends
                                for friend in
                                    person_map.get(&interest_person_id).unwrap().friends.clone()
                                {
                                    candidate_friends.remove(&friend);
                                }
                                candidate_friends.remove(&interest_person_id);

                                // Find the forums your friends like
                                let mut candidate_friends_forums = HashMap::new();
                                for candidate in &candidate_friends {
                                    candidate_friends_forums.insert(
                                        candidate,
                                        person_forums
                                            .get(&candidate)
                                            .unwrap()
                                            .intersection(
                                                person_forums.get(&interest_person_id).unwrap(),
                                            )
                                            .collect::<Vec<_>>()
                                            .len(),
                                    );
                                }

                                // Find the number of common friends
                                let mut candidate_friends_mutual_friends = HashMap::new();
                                for candidate in &candidate_friends {
                                    let mut friends1 = person_map
                                        .get(&interest_person_id)
                                        .unwrap()
                                        .friends
                                        .clone();
                                    let mut friends2 = match person_map.get(&candidate) {
                                        Some(candidate) => candidate.friends.clone(),
                                        None => Vec::new(),
                                    };

                                    let friends1_set: HashSet<u32> =
                                        HashSet::from_iter(friends1.drain(..));
                                    let friends2_set: HashSet<u32> =
                                        HashSet::from_iter(friends2.drain(..));

                                    candidate_friends_mutual_friends.insert(
                                        candidate,
                                        friends1_set
                                            .intersection(&friends2_set)
                                            .collect::<Vec<_>>()
                                            .len(),
                                    );
                                }

                                // find top friends
                                let mut candidate_metrics = HashMap::new();
                                for candidate in &candidate_friends {
                                    // compute metric
                                    let metric = candidate_friends_forums.get(&candidate).unwrap()
                                        + candidate_friends_mutual_friends.get(&candidate).unwrap();

                                    candidate_metrics.insert(candidate, metric);
                                }

                                let mut recommendations = Vec::new();
                                for _ in 0..RECOMMENDATIONS {
                                    let mut best_candidate = None;
                                    let mut best_metric = 0;
                                    for (candidate, metric) in &candidate_metrics {
                                        if *metric > best_metric {
                                            best_metric = *metric;
                                            best_candidate = Some(*candidate);
                                        }
                                    }

                                    if let Some(candidate) = best_candidate {
                                        candidate_metrics.remove(&candidate);
                                        recommendations.push(*candidate);
                                    }
                                }

                                let mut session = output.session(&cap);
                                session.give((interest_person_id, recommendations));
                            }
                        })
                    },
                )
                .inspect_batch(|t, xs| println!("Recommendations @{:?}: {:?}", t, xs));
        });
    })
    .unwrap();
}
