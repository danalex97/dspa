use crate::dto::forum::Forum;
use crate::dto::person::Person;
use std::collections::HashMap;

pub fn parse_forum_member_csv(file: &str, forum_map: &mut HashMap<u32, Forum>) {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_path(file)
        .unwrap();
    for record in rdr.records() {
        match record {
            Ok(r) => match forum_map.get_mut(&(r[0].parse().unwrap())) {
                Some(forum) => {
                    forum.add_member(r[1].parse().unwrap());
                }
                None => eprintln!("Attempting to add member to unrecognised forum #{}", &r[0]),
            },
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }
}

pub fn parse_person_friends(file: &str, person_map: &mut HashMap<u32, Person>) {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_path(file)
        .unwrap();
    for record in rdr.records() {
        match record {
            Ok(r) => match person_map.get_mut(&(r[0].parse().unwrap())) {
                Some(person) => {
                    person.add_friend(r[1].parse().unwrap());
                }
                None => eprintln!("Attempting to add friend to unrecognised person #{}", &r[0]),
            },
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }
}
