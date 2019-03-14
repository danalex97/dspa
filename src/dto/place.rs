extern crate csv;

use super::common::Importable;
use super::common::PlaceType;

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

impl Importable<Place> for Place {
    fn from_record(record: StringRecord) -> Result<Place, Box<Error>> {
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

    fn id(&self) -> Option<u32> {
        Some(self.id)
    }
}
