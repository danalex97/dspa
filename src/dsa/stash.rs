use std::collections::HashMap;
use std::fmt::Debug;

pub type Stash<T> = HashMap<usize, Vec<T>>;

pub trait Stashable<T> {
    fn stash(&mut self, time: usize, value: T);
    fn extract(&mut self, length: usize, time_end: usize) -> Vec<T>;
}

impl<T: Debug> Stashable<T> for Stash<T> {
    // Stash at time `time`.
    fn stash(&mut self, time: usize, value: T) {
        self.entry(time).or_insert(vec![]).push(value);
    }

    // Extracts everything on the interval [time_end-length, time_end)
    fn extract(&mut self, length: usize, time_end: usize) -> Vec<T> {
        let mut all = Vec::new();
        for t in time_end - length..time_end {
            match self.remove(&t) {
                Some(mut vec) => all.append(&mut vec),
                None => {}
            }
        }
        all
    }
}

#[test]
fn test_stash_doesnt_take_last_element() {
    let mut stash: Stash<u32> = Stash::new();

    stash.stash(0, 1);
    stash.stash(1, 2);
    stash.stash(2, 3);
    stash.stash(3, 4);

    let vec = stash.extract(3, 3);
    assert!(vec == vec![1, 2, 3]);
}

#[test]
fn test_stashed_elements_are_consumed() {
    let mut stash: Stash<u32> = Stash::new();

    stash.stash(0, 1);
    stash.stash(1, 2);
    stash.stash(2, 3);
    stash.stash(3, 4);

    let vec = stash.extract(3, 3);
    assert!(vec == vec![1, 2, 3]);

    stash.stash(3, 4);
    stash.stash(3, 1);

    let vec = stash.extract(3, 3);
    let vec2: Vec<u32> = vec![];
    assert!(vec == vec2);

    let vec = stash.extract(1, 4);
    assert!(vec == vec![4, 4, 1]);
}
