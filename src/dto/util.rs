use std::error::Error;
use std::str::FromStr;

use std::option::Option;

pub fn maybe_record<T>(raw_record: String) -> Option<T> where
        T: FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug, {
    if raw_record.is_empty() {
        None
    } else {
        Some(raw_record.parse::<T>().unwrap())
    }
}
