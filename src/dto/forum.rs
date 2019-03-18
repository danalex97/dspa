extern crate chrono;
extern crate csv;

use crate::dto::common::Importable;

use chrono::{DateTime, FixedOffset};
use csv::StringRecord;
use std::error::Error;

#[derive(Debug)]
pub struct Forum {
    pub id: u32,
    pub title: String,
    pub creation_date: DateTime<FixedOffset>,
    pub member_ids: Vec<u32>,
}

impl Importable<Forum> for Forum {
    fn from_record(record: StringRecord) -> Result<Forum, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let title = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;

        Ok(Forum {
            id,
            title,
            creation_date,
            member_ids: Vec::new(),
        })
    }

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}

impl Forum {
    pub fn add_member(&mut self, id: u32) {
        self.member_ids.push(id);
    }
}
