extern crate chrono;
extern crate csv;

use crate::dto::common::maybe_record;
use crate::dto::common::Browser;
use crate::dto::common::Importable;

use chrono::{DateTime, FixedOffset};
use csv::StringRecord;

use std::error::Error;
use std::option::Option;

#[derive(Debug)]
pub struct Comment {
    pub id: u32,
    pub person_id: u32,
    pub creation_date: DateTime<FixedOffset>,
    pub location_ip: std::net::Ipv4Addr,
    pub browser_used: Browser,
    pub content: String,
    pub reply_to_post_id: Option<u32>,
    pub reply_to_comment_id: Option<u32>,
    pub place_id: u32,
}

impl Importable<Comment> for Comment {
    fn from_record(record: StringRecord) -> Result<Comment, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let person_id: u32 = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;
        let location_ip = record[3].parse()?;
        let browser_used = record[4].parse()?;
        let content = record[5].parse()?;
        let reply_to_post_id = maybe_record::<u32>(record[6].parse()?)?;
        let reply_to_comment_id = maybe_record::<u32>(record[7].parse()?)?;
        let place_id = record[8].parse()?;

        Ok(Comment{
            id,
            person_id,
            creation_date,
            location_ip,
            browser_used,
            content,
            reply_to_post_id,
            reply_to_comment_id,
            place_id,
        })
    }

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}
