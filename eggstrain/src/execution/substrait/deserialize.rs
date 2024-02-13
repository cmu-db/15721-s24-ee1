use std::fs;
use substrait::proto::{Plan, ReadRel};

pub fn read_str(path: &str) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| panic!("Unable to read file {}", path))
}

pub fn get_plan(path: &str) -> Plan {
    let plan = serde_json::from_str::<Plan>(&read_str(path))
        .unwrap_or_else(|_| panic!("Could not parse json {:?} into Plan", path));
    println!("{}", serde_json::to_string_pretty(&plan).unwrap());
    plan
}

pub fn get_read(path: &str) -> ReadRel {
    let read = serde_json::from_str::<ReadRel>(&read_str(path))
        .unwrap_or_else(|_| panic!("Could not parse json {:?} into Plan", path));
    println!("{}", serde_json::to_string_pretty(&read).unwrap());
    read
}
