use std::collections::HashMap;

pub type Stash<T> = HashMap<usize, Vec<T>>;

pub trait Stashable<T> {
    fn stash(&mut self, time: usize, value: T);
    fn extract(&mut self, length: usize, time_end: usize) -> Vec<T>;
}

impl<T> Stashable<T> for Stash<T> {
    fn stash(&mut self, time: usize, value: T) {
        self.entry(time).or_insert(vec![]).push(value);
    }

    fn extract(&mut self, length: usize, time_end: usize) -> Vec<T> {
        let mut all = Vec::new();
        for t in time_end - length..time_end {
            self.entry(t).and_modify(|vec| all.extend(vec.drain(..)));
        }
        all
    }
}
