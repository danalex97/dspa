use std::collections::HashMap;
use std::str::FromStr;

use super::dto::common::Importable;

pub fn parse_csv<T>(file: &str) -> HashMap<u32, T> where
        T: Importable<T>,
        T: std::fmt::Debug, {
    let mut map = HashMap::new();
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'|').from_path(file).unwrap();
    for record in rdr.records() {
        match record {
            Ok(record) => {
                let data = T::from_record(record);
                if data.is_ok() {
                    let entry = data.unwrap();;
                    map.insert(entry.id().unwrap(), entry);
                } else {
                    eprintln!("Error: {:?}", data);
                }
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }
    map
}
