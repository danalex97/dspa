use std::collections::HashMap;
use std::str::FromStr;

use dto::person::Person;
use dto::place::Place;
use dto::forum::Forum;

pub fn parse_persons_csv(file: &str) -> HashMap<u32, Person> {
    let mut map = HashMap::new();
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'|').from_path(file).unwrap();
    for record in rdr.records() {
        match record {
            Ok(r) => {
                let p = Person::from_record(r);
                if p.is_ok() {
                    let person = p.unwrap();
                    map.insert(person.id, person);
                } else {
                    eprintln!("Error: {:?}", p);
                }
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }
    map
}

pub fn parse_place_csv(file: &str) -> HashMap<u32, Place> {
    let mut map = HashMap::new();
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'|').from_path(file).unwrap();
    for record in rdr.records() {
        match record {
            Ok(r) => {
                let p = Place::from_record(r);
                if p.is_ok() {
                    let place = p.unwrap();
                    map.insert(place.id, place);
                } else {
                    eprintln!("Error: {:?}", p);
                }
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }
    map
}

pub fn parse_forum_csv(file: &str) -> HashMap<u32, Forum> {
    let mut map = HashMap::new();
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'|').from_path(file).unwrap();
    for record in rdr.records() {
        match record {
            Ok(r) => {
                let f = Forum::from_record(r);
                if f.is_ok() {
                    let forum = f.unwrap();
                    map.insert(forum.id, forum);
                } else {
                    eprintln!("Error: {:?}", f);
                }
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }
    map
}
