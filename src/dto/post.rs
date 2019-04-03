extern crate chrono;
extern crate csv;

use crate::dto::common::Importable;
use crate::dto::common::maybe_record;
use crate::dto::common::parse_vector;

use chrono::{DateTime, FixedOffset};
use csv::StringRecord;

use std::error::Error;
use std::vec::Vec;

#[derive(Debug, Clone)]
pub struct Post {
    pub id: u32,
    pub person_id: u32,
    pub creation_date: DateTime<FixedOffset>,
    pub image_file: Option<String>,
    pub location_ip: std::net::Ipv4Addr,
    pub browser_used: String,
    pub language: String,
    pub content: String,
    pub tags: Vec<u32>,
    pub forum_id: u32,
    pub place_id: u32,
}

impl Importable<Post> for Post {
    fn from_record(record: StringRecord) -> Result<Post, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let person_id: u32 = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;
        let image_file = maybe_record::<String>(&record[3]);
        let location_ip = record[4].parse()?;
        let browser_used = record[5].parse()?;
        let language = record[6].parse()?;
        let content = record[7].parse()?;
        let tags = parse_vector::<u32>(&record[8])?;
        let forum_id = record[9].parse()?;
        let place_id = record[10].parse()?;

        Ok(Post{
            id,
            person_id,
            creation_date,
            image_file,
            location_ip,
            browser_used,
            language,
            content,
            tags,
            forum_id,
            place_id,
        })
    }

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}
