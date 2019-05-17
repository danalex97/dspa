extern crate chrono;
extern crate csv;

use crate::dto::common::{Importable, Timestamped, Watermarkable};

use chrono::DateTime;
use csv::StringRecord;
use std::error::Error;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Like {
    pub person_id: u32,
    pub post_id: u32,
    pub timestamp: usize,
    pub is_watermark: bool,
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
            is_watermark: false,
        })
    }

    fn id(&self) -> Option<u32> {
        None
    }
}

impl Watermarkable for Like {
    fn from_watermark(watermark: &str) -> Like {
        Like {
            person_id: 0,
            post_id: 0,
            timestamp: watermark.parse().unwrap(),
            is_watermark: true,
        }
    }

    fn is_watermark(&self) -> bool {
        self.is_watermark
    }
}

impl Timestamped for Like {
    fn timestamp(&self) -> usize {
        self.timestamp
    }
}
