extern crate chrono;
extern crate csv;

use crate::Browser;

use chrono::{NaiveDate, DateTime, FixedOffset};
use csv::StringRecord;
use std::error::Error;
use std::str::FromStr;

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

#[derive(Debug)]
pub enum Gender {
    MALE,
    FEMALE,
}

impl Person {
    pub fn from_record(record: StringRecord) -> Result<Person, Box<Error>> {
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
}

impl FromStr for Gender {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("male") {
            return Ok(Gender::MALE);
        }
        if s.eq("female") {
            return Ok(Gender::FEMALE);
        }
        return Err("Invalid gender specified");
    }
}
