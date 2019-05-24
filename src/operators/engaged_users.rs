extern crate timely;

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::{Scope, Stream};

use std::collections::HashMap;
use std::collections::HashSet;

type PostId = u32;
#[allow(dead_code)]
type PersonId = u32;

pub trait EngagedUsers<G, P>
where
    G: Scope,
    P: ParallelizationContract<usize, (PostId, HashSet<PersonId>)>,
{
    // Returns a tuple of (post_id, people_engaged_with_post)
    fn engaged_users(&self, p_pact: P, collection_period: usize) -> Stream<G, (PostId, usize)>;
}

impl<G, P> EngagedUsers<G, P> for Stream<G, (PostId, HashSet<PersonId>)>
where
    G: Scope<Timestamp = usize>,
    P: ParallelizationContract<usize, (PostId, HashSet<PersonId>)>,
{
    fn engaged_users(&self, p_pact: P, collection_period: usize) -> Stream<G, (PostId, usize)> {
        let mut first_notified_engaged = false;
        let mut engaged = HashMap::new();
        let mut active_post_snapshot = Vec::new();

        self.unary_notify(
            p_pact,
            "Unique People Counts",
            None,
            move |p_input, output, notificator| {
                // actual processing
                p_input.for_each(|cap, input| {
                    input.swap(&mut active_post_snapshot);
                    for (post_id, engaged_people) in active_post_snapshot.clone() {
                        let entry = engaged.entry(post_id).or_insert(engaged_people.len());
                        *entry = engaged_people.len();
                    }

                    // Generate the first notification
                    if !first_notified_engaged {
                        notificator.notify_at(cap.delayed(&(cap.time() + collection_period)));
                        first_notified_engaged = true;
                    }
                });

                notificator.for_each(|cap, _, notificator| {
                    notificator.notify_at(cap.delayed(&(cap.time() + collection_period)));

                    let mut session = output.session(&cap);
                    for (post_id, _) in active_post_snapshot.drain(..) {
                        let engaged_users = match engaged.get(&post_id) {
                            Some(value) => *value,
                            None => 0,
                        };
                        session.give((post_id, engaged_users));
                    }
                });
            },
        )
    }
}
