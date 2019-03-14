extern crate csv;

use csv::StringRecord;
use std::error::Error;
use std::str::FromStr;

#[derive(Debug)]
pub struct Place {
    pub id: u32,
    pub name: String,
    pub url: String,
    pub place_type: PlaceType,
}

impl Place {
    pub fn from_record(record: StringRecord) -> Result<Place, Box<Error>> {
        let id: u32 = record[0].parse()?;
        let name = record[1].parse()?;
        let url = record[2].parse()?;
        let place_type = record[3].parse()?;

        Ok(Place {
            id,
            name,
            url,
            place_type,
        })

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
