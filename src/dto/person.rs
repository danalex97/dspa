extern crate chrono;
extern crate csv;

use crate::dto::common::Browser;
use crate::dto::common::Gender;
use crate::dto::common::Importable;

use chrono::{DateTime, FixedOffset, NaiveDate};
use csv::StringRecord;
use std::error::Error;

#[derive(Debug)]
pub struct Person {
    pub id: u32,
    pub first_name: String,
    pub last_name: String,
    pub gender: Gender,
    pub birthday: NaiveDate,
    pub creation_date: DateTime<FixedOffset>,
    pub location_ip: std::net::Ipv4Addr,
    pub browser_used: Browser,
}

impl Importable<Person> for Person {
    fn from_record(record: StringRecord) -> Result<Person, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let first_name = record[1].parse()?;
        let last_name = record[2].parse()?;
        let gender = record[3].parse()?;
        let birthday = NaiveDate::parse_from_str(&record[4], "%Y-%m-%d")?;
        let creation_date = DateTime::parse_from_rfc3339(&record[5])?;
        let location_ip = record[6].parse()?;
        let browser_used = record[7].parse()?;

        Ok(Person {
            id,
            first_name,
            last_name,
            gender,
            birthday,
            creation_date,
            location_ip,
            browser_used,
        })
    }

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}
