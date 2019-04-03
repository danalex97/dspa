extern crate csv;
use csv::StringRecord;

use std::str::FromStr;
use std::vec::Vec;
use std::error::Error;

pub trait Importable<T> {
    fn from_record(raw_record: StringRecord) -> Result<T, Box<Error>>;
    fn id(&self) -> Option<u32>;
}

pub fn maybe_record<T>(raw_record: &str) -> Option<T> where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug, {
    let parsed = raw_record.parse::<T>();
    match parsed {
        Ok(r) => Some(r),
        Err(_) => None
    }
}

pub fn parse_vector<T>(raw_vector: &str) -> Result<Vec<T>, Box<Error>> where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug, {
    if raw_vector.is_empty() {
        return Ok(Vec::new())
    }

    Ok(raw_vector.trim()
                 .replace("[", "")
                 .replace("]", "")
                 .split(", ")
                 .map(|x| {x.parse::<T>().unwrap()})
                 .collect()
    )
}

#[derive(Debug)]
pub enum Browser {
    Chrome,
    Firefox,
    InternetExplorer,
    Opera,
    Safari,
}

impl FromStr for Browser {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("Firefox") {
            return Ok(Browser::Firefox);
        }
        if s.eq("Chrome") {
            return Ok(Browser::Chrome);
        }
        if s.eq("Safari") {
            return Ok(Browser::Safari);
        }
        if s.eq("Opera") {
            return Ok(Browser::Opera);
        }
        if s.eq("Internet Explorer") {
            return Ok(Browser::InternetExplorer);
        }
        println!("{:?}", s);
        Err("Unrecognised browser")
    }
}

#[derive(Debug)]
pub enum Gender {
    MALE,
    FEMALE,
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

#[derive(Debug)]
pub enum PlaceType {
    CONTINENT,
    COUNTRY,
    CITY,
}

impl FromStr for PlaceType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq("continent") {
            return Ok(PlaceType::CONTINENT);
        }
        if s.eq("country") {
            return Ok(PlaceType::COUNTRY);
        }
        if s.eq("city") {
            return Ok(PlaceType::CITY);
        }
        return Err("Invalid place type specified");
    }
}
