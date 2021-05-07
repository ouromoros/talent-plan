use bson::Document;
use serde::{Deserialize, Serialize};
use std::{fs::File};
#[derive(Debug, Serialize, Deserialize)]
struct Move {
    x: i32,
    y: i32,
}
fn main() {
    bson_1000_file()
}

fn json_exercise() {
    {
        let a = Move { x: 1, y: -1 };
        println!("a = {:?}", a);
        let file = File::create("temp_data").unwrap();
        serde_json::to_writer(file, &a).unwrap();
    }
    {
        let file = File::open("temp_data").unwrap();
        let b: Move = serde_json::from_reader(file).unwrap();
        println!("b = {:?}", b);
    }
}

fn ron_exercise() {
    let a = Move { x: 1, y: -1 };
    println!("a = {:?}", a);
    let mut buf: Vec<u8> = Vec::new();
    let serialized = ron::to_string(&a).unwrap();
    buf.extend(serialized.into_bytes());

    let data = std::str::from_utf8(&buf).unwrap();
    println!("data = {:?}", data);
    let b: Move = ron::from_str(data).unwrap();
    println!("b = {:?}", b);
}

fn bson_1000_file() {
    {
        let mut file = File::create("temp_data").unwrap();
        for i in 1..1001 {
            let a = Move { x: i, y: i };
            bson::to_document(&a).unwrap().to_writer(&mut file).unwrap();
        }
    }
    {
        let mut file = File::open("temp_data").unwrap();
        for i in 1..1001 {
            let d = bson::Document::from_reader(&mut file).unwrap();
            let b: Move = bson::from_bson(bson::Bson::Document(d)).unwrap();
            assert_eq!(b.x, i);
            assert_eq!(b.y, i);
        }
    } 
}
