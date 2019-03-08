
pub use crate::person::Person;
use std::collections::HashMap;
use std::str::FromStr;

pub mod person;

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