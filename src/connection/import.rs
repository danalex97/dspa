use std::collections::HashMap;
use crate::dto::common::Importable;

// function that parses a CSV and applies callback for each record
pub fn parse_csv<T, F>(file: &str, mut callback: F) where
        T: Importable<T>,
        T: std::fmt::Debug,
        F: FnMut(T) {
    let mut rdr = csv::ReaderBuilder::new().delimiter(b'|').from_path(file).unwrap();
    for record in rdr.records() {
        match record {
            Ok(record) => {
                let data = T::from_record(record);
                if data.is_ok() {
                    let entry = data.unwrap();
                    callback(entry);
                } else {
                    eprintln!("Error: {:?}", data);
                }
            },
            Err(e) => eprintln!("Error: {:?}", e)
        }
    }
}

pub fn csv_to_map<T>(file: &str) -> HashMap<u32, T> where
        T: Importable<T>,
        T: std::fmt::Debug, {
    let mut map = HashMap::new();
    parse_csv(file, |entry : T| {
        map.insert(entry.id().unwrap(), entry);
    });
    map
}
