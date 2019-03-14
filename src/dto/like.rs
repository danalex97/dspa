extern crate chrono;
extern crate csv;

use chrono::{DateTime, FixedOffset};
use csv::StringRecord;

use std::option::Option;

#[derive(Debug)]
pub struct Like {
    pub person_id: u32,
    pub post_id: u32,
    pub creation_date: DateTime<FixedOffset>,
}

impl Like {
    pub fn from_record(record: StringRecord) -> Result<Comment, Box<Error>> {
        let person_id = record[0].parse()?;
        let post_id = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;

        Ok(Likes{
            person_id,
            post_id,
            creation_date,
        })
    }
}
