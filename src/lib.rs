
pub use crate::person::Person;
pub use crate::place::Place;
pub use crate::forum::Forum;
use std::collections::HashMap;
use std::str::FromStr;

pub mod person;
pub mod place;
pub mod forum;

#[derive(Debug)]
pub enum Browser {
    Chrome,
    Firefox,
    InternetExplorer,
    Safari,
}


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

impl FromStr for Browser {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("Firefox") {
            return Ok(Browser::Firefox);
        }
        if s.eq("Chrome") {
            return Ok(Browser::Chrome);
        }
        if s.eq("Safari") {
            return Ok(Browser::Safari);
        }
        if s.eq("Internet Explorer") {
            return Ok(Browser::InternetExplorer);
        }
        Err("Unrecognised browser")
    }
}