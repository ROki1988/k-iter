#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rusoto_core;
extern crate rusoto_kinesis;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

mod cli;
mod kinesis;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;

use cli::IteratorType;
use kinesis::KinesisIterator;
use rusoto_core::Region;
use rusoto_kinesis::Record;

#[derive(Debug, Clone, Serialize)]
pub struct RecordRef<'a> {
    #[serde(rename = "ApproximateArrivalTimestamp")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approximate_arrival_timestamp: &'a Option<f64>,
    #[serde(rename = "Data")]
    pub data: &'a [u8],
    #[serde(rename = "EncryptionType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_type: &'a Option<String>,
    #[serde(rename = "PartitionKey")]
    pub partition_key: &'a String,
    #[serde(rename = "SequenceNumber")]
    pub sequence_number: &'a String,
}

impl<'a> RecordRef<'a> {
    fn new(origin: &'a Record) -> Self {
        RecordRef {
            approximate_arrival_timestamp: &origin.approximate_arrival_timestamp,
            data: origin.data.as_slice(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }
}

fn records2string_only_data(records: &Vec<Record>) -> String {
    records
        .iter()
        .map(|x| String::from_utf8_lossy(&x.data).to_string())
        .collect::<Vec<String>>()
        .join("\n")
}

fn records2string_verbose(records: &Vec<Record>) -> String {
    records
        .iter()
        .filter_map(|x| serde_json::to_string(&RecordRef::new(x)).ok())
        .collect::<Vec<String>>()
        .join("\n")
}

fn main() {
    let matches = cli::build_app().get_matches();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let name = value_t_or_exit!(matches.value_of("stream-name"), String);
    let id = value_t_or_exit!(matches.value_of("shard-id"), String);
    let region = value_t_or_exit!(matches.value_of("region"), Region);
    let iter_type: IteratorType = value_t_or_exit!(matches.value_of("iterator-type"), IteratorType);

    let printer = if matches.is_present("verbose") {
        records2string_verbose
    } else {
        records2string_only_data
    };

    let mut it = match iter_type {
        IteratorType::LATEST | IteratorType::TRIM_HORIZON => {
            KinesisIterator::new(name, id, iter_type.to_string(), region)
        }
        IteratorType::AT_SEQUENCE_NUMBER | IteratorType::AFTER_SEQUENCE_NUMBER => {
            let seq = value_t_or_exit!(matches.value_of("sequence-number"), String);
            KinesisIterator::new_with_sequence_number(name, id, iter_type.to_string(), seq, region)
        }
        IteratorType::AT_TIMESTAMP => {
            let timestamp = value_t_or_exit!(matches.value_of("timestamp"), f64);
            KinesisIterator::new_with_timestamp(name, id, iter_type.to_string(), timestamp, region)
        }
    };

    while running.load(Ordering::SeqCst) {
        if let Some(Ok(n)) = it.next() {
            thread::sleep(time::Duration::from_millis(1000));
            if !n.records.is_empty() {
                println!("{}", printer(&n.records));
            }
        }
    }
}
