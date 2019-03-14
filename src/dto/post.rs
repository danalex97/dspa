extern crate chrono;
extern crate csv;

use super::util::maybe_record;

use chrono::{DateTime, FixedOffset};
use csv::StringRecord;

use std::error::Error;
use std::option::Option;
use std::vec::Vec;

// id|personId|creationDate|imageFile|locationIP|browserUsed|language|content|tags|forumId|placeId
// 270250|122|2012-02-02T02:46:56Z||204.79.194.17|Firefox|en|About Vladimir Lenin, of Marxism that emphasized the critical role played. About Kurt Weill, Weill (March 2, 1900 � April 3, 1950) was a.|[7001, 1667]|2440|28
// 1

#[derive(Debug)]
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

impl Post {
    pub fn from_record(record: StringRecord) -> Result<Post, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let person_id: u32 = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;
        let image_file = maybe_record::<String>(record[3].parse()?);
        let location_ip = record[4].parse()?;
        let browser_used = record[5].parse()?;
        let language = record[6].parse()?;
        let content = record[7].parse()?;
        let tags = vec![];
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
}
