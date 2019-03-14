use std::str::FromStr;
use std::vec::Vec;
use std::option::Option;
use std::error::Error;

pub fn maybe_record<T>(raw_record: String) -> Result<Option<T>, Box<Error>> where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug, {
    if raw_record.is_empty() {
        Ok(None)
    } else {
        Ok(Some(raw_record.parse::<T>().unwrap()))
    }
}

pub fn parse_vector<T>(raw_vector: String) -> Result<Vec<T>, Box<Error>> where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug, {
    Ok(raw_vector.trim().replace("[", "").split(",").map(|x| x.parse::<T>().unwrap()).collect())
}
