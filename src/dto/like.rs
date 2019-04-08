extern crate chrono;
extern crate csv;

use crate::dto::common::{Importable, Timestamped};

use chrono::DateTime;
use csv::StringRecord;
use std::error::Error;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Like {
    pub person_id: u32,
    pub post_id: u32,
    pub timestamp: usize,
}

impl Importable<Like> for Like {
    fn from_record(record: StringRecord) -> Result<Like, Box<Error>> {
        let person_id = record[0].parse()?;
        let post_id = record[1].parse()?;
        let creation_date = DateTime::parse_from_rfc3339(&record[2])?;

        Ok(Like {
            person_id,
            post_id,
            timestamp: creation_date.timestamp() as usize,
        })
    }

    fn id(&self) -> Option<u32> {
        None
    }
}

impl Timestamped for Like {
    fn timestamp(&self) -> usize {
        self.timestamp
    }
}
