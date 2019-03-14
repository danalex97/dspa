mod dto;

use dto::comment::Comment;
use csv::StringRecord;

fn main() {
    let raw_record: &str      = "46228400|9106|2010-02-02T00:06:24Z|31.31.96.17|Firefox|About André Sá, Sá is a professional Brazilian. About Gustav Mahler, performance standards ensured his. About Thomas Aquinas, seminar method. It has school.||46228380|102";
    let vec_record: Vec<&str> = raw_record.split('|').collect();
    let c = Comment::from_record(StringRecord::from(vec_record));
    println!("{:?}", c);
}
