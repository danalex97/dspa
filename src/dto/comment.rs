extern crate chrono;
extern crate csv;

use crate::dto::common::maybe_record;
use crate::dto::common::Browser;
use crate::dto::common::{Importable, Timestamped};

use chrono::{DateTime, FixedOffset};
use csv::StringRecord;

use std::error::Error;
use std::option::Option;
use std::cmp::Ordering;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Comment {
    pub id: u32,
    pub person_id: u32,
    pub timestamp: usize,
    pub location_ip: std::net::Ipv4Addr,
    pub browser_used: Browser,
    pub content: String,
    pub reply_to_post_id: Option<u32>,
    pub reply_to_comment_id: Option<u32>,
    pub place_id: u32,
    pub replies: Vec<Comment>,
}

impl Importable<Comment> for Comment {
    fn from_record(record: StringRecord) -> Result<Comment, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let person_id: u32 = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;
        let location_ip = record[3].parse()?;
        let browser_used = record[4].parse()?;
        let content = record[5].parse()?;
        let reply_to_post_id = maybe_record::<u32>(&record[6]);
        let reply_to_comment_id = maybe_record::<u32>(&record[7]);
        let place_id = record[8].parse()?;

        Ok(Comment{
            id,
            person_id,
            timestamp: creation_date.timestamp() as usize,
            location_ip,
            browser_used,
            content,
            reply_to_post_id,
            reply_to_comment_id,
            place_id,
            replies: vec![],
        })
    }

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}

impl Timestamped for Comment {
    fn timestamp(&self) -> usize {
        self.timestamp
    }
}

impl Ord for Comment {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for Comment {
    fn partial_cmp(&self, other: &Comment) -> Option<Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl Eq for Comment {

}

impl PartialEq for Comment {
    fn eq(&self, other: &Comment) -> bool {
        self.timestamp.eq(&other.timestamp)
    }
}
