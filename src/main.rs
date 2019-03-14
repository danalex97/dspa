mod dto;
mod stream;
mod import;
mod producer;

use dto::comment::Comment;
use dto::person::Person;
use dto::common::Importable;
use csv::StringRecord;
use stream::listen;
use import::parse_csv;
use producer::producer;

fn main() {
    let raw_record: &str      = "46228400|9106|2010-02-02T00:06:24Z|31.31.96.17|Firefox|About André Sá, Sá is a professional Brazilian. About Gustav Mahler, performance standards ensured his. About Thomas Aquinas, seminar method. It has school.||46228380|102";
    let vec_record: Vec<&str> = raw_record.split('|').collect();
    let c = Comment::from_record(StringRecord::from(vec_record));
    println!("{:?}", c);
    println!();

    producer();
    listen();
    parse_csv::<Person>("data/1k-users-sorted/tables/person.csv");
}
