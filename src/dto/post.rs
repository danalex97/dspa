extern crate chrono;
extern crate csv;

use crate::dto::common::{maybe_record, Watermarkable};
use crate::dto::common::parse_vector;
use crate::dto::common::{Importable, Timestamped};

use chrono::DateTime;
use csv::StringRecord;

use std::error::Error;
use std::vec::Vec;
use std::net::Ipv4Addr;

#[derive(Deserialize, Serialize, Clone, Debug, Eq, Hash, PartialEq)]
pub struct Post {
    pub id: u32,
    pub person_id: u32,
    pub timestamp: usize,
    pub image_file: Option<String>,
    pub location_ip: std::net::Ipv4Addr,
    pub browser_used: String,
    pub language: String,
    pub content: String,
    pub tags: Vec<u32>,
    pub forum_id: u32,
    pub place_id: u32,
    pub likes: u32,
    pub is_watermark: bool,
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

        Ok(Post {
            id,
            person_id,
            timestamp: creation_date.timestamp() as usize,
            image_file,
            location_ip,
            browser_used,
            language,
            content,
            tags,
            forum_id,
            place_id,
            likes: 0,
            is_watermark: false,
        })
    }

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}

impl Watermarkable for Post {

    fn from_watermark(watermark: &str) -> Post {
        Post {
            id: 0,
            person_id: 0,
            timestamp: watermark.parse().unwrap(),
            image_file: None,
            location_ip: Ipv4Addr::new(0,0,0,0),
            browser_used: "".to_string(),
            language: "".to_string(),
            content: "".to_string(),
            tags: vec![],
            forum_id: 0,
            place_id: 0,
            likes: 0,
            is_watermark: true
        }
    }

}

impl Timestamped for Post {
    fn timestamp(&self) -> usize {
        self.timestamp
    }
}
