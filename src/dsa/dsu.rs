use std::collections::HashMap;
use std::hash::Hash;

struct Node<K, V> {
    parent : usize,
    key : K,
    value : V,
}

pub struct Dsu<K : Hash + Eq + Clone, V> {
    to_id : HashMap<K, usize>,
    data : Vec<Node<K, V> >,
}

impl<K : Hash + Eq + Clone, V> Dsu<K, V> {
    fn new() -> Dsu<K, V> {
        Dsu {
            to_id : HashMap::new(),
            data : vec![],
        }
    }

    fn get(&mut self, id : usize) -> &mut Node<K, V> {
        // find root
        let mut root = id;

        while self.data[root].parent != root {
            root = self.data[root].parent;
        }

        // compress path
        let mut cur = id;
        while self.data[cur].parent != root {
            let next = self.data[cur].parent;
            self.data[cur].parent = root;
            cur = next;
        }

        // return relevant node
        &mut self.data[root]
    }

    pub fn key(&mut self, key : K) -> Option<&K> {
        match self.to_id.get(&key) {
            Some(id) => Some(&self.get(*id).key),
            None => None,
        }
    }

    pub fn value(&mut self, key : K) -> Option<&V> {
        match self.to_id.get(&key) {
            Some(&id) => Some(&self.get(id).value),
            None => None,
        }
    }

    pub fn union(&mut self, lhs : K, rhs : K) {
        match self.to_id.get(&lhs) {
            Some(&lhs) => match self.to_id.get(&rhs) {
                Some(&rhs) => {
                    // If both sides are ok, make a union
                    self.get(rhs).parent = self.get(lhs).parent;
                },
                None => {}
            },
            None => {}
        }
    }

    pub fn insert(&mut self, key : K, value : V) {
        let k = key.clone();
        let id = *self.to_id
            .entry(key)
            .or_insert(self.data.len());

        if id == self.data.len() {
            // new value
            self.data.push(Node{
                parent : id,
                key : k,
                value : value,
            });
        } else {
            // different value
            self.get(id).value = value;
        }
    }
}

#[test]
fn test_dsu() {
    let mut dsu: Dsu<usize, &str> = Dsu::new();

    dsu.insert(2, "a");
    dsu.insert(5, "b");
    dsu.insert(1, "c");

    assert!(dsu.key(2) == Some(&2));
    assert!(dsu.key(5) == Some(&5));
    assert!(dsu.key(1) == Some(&1));
    assert!(dsu.key(3) == None);

    assert!(dsu.value(2) == Some(&"a"));
    assert!(dsu.value(5) == Some(&"b"));
    assert!(dsu.value(1) == Some(&"c"));
    assert!(dsu.value(3) == None);

    dsu.union(2, 5);

    assert!(dsu.value(2) == Some(&"a"));
    assert!(dsu.value(5) == Some(&"a"));

    dsu.insert(2, "d");
    assert!(dsu.value(2) == Some(&"d"));
    assert!(dsu.value(5) == Some(&"d"));
}
