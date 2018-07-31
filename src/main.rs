#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rusoto_core;
extern crate rusoto_kinesis;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;

use clap::{App, Arg};

use rusoto_core::Region;
use rusoto_kinesis::{GetRecordsError, GetRecordsInput, Record, GetRecordsOutput, GetShardIteratorError,
                     GetShardIteratorInput, Kinesis, KinesisClient};

#[derive( Debug, Clone, Serialize)]
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

pub struct KinesisIterator {
    client: KinesisClient,
    input: GetShardIteratorInput,
    token: Option<String>,
}

impl KinesisIterator {
    fn new_self(input: GetShardIteratorInput, region: Region) -> Self {
        let c = KinesisClient::simple(region);
        KinesisIterator {
            client: c,
            input,
            token: None,
        }
    }

    pub fn new(
        stream_name: String,
        shard_id: String,
        shard_iterator_type: IteratorType,
        region: Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name,
            ..Default::default()
        };
        KinesisIterator::new_self(input, region)
    }

    pub fn new_with_sequence_number(
        stream_name: String,
        shard_id: String,
        shard_iterator_type: IteratorType,
        sequence_number: String,
        region: Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name,
            starting_sequence_number: Some(sequence_number),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region)
    }

    pub fn new_with_timestamp(
        stream_name: String,
        shard_id: String,
        shard_iterator_type: IteratorType,
        timestamp: f64,
        region: Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name,
            timestamp: Some(timestamp),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region)
    }

    pub fn get_iterator_token(&self) -> Result<Option<String>, GetShardIteratorError> {
        self.client
            .get_shard_iterator(&self.input)
            .sync()
            .map(|x| x.shard_iterator)
    }
}

impl Iterator for KinesisIterator {
    type Item = Result<GetRecordsOutput, GetRecordsError>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.token
            .clone()
            .or_else(|| self.get_iterator_token().unwrap())
            .map(|x| {
                self.token = Some(x.clone());
                let r = GetRecordsInput {
                    shard_iterator: x,
                    ..Default::default()
                };
                self.client.get_records(&r).sync().map(|x| {
                    self.token = x.next_shard_iterator.clone();
                    x
                })
            })
    }
}

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum IteratorType {
        LATEST,
        AT_SEQUENCE_NUMBER,
        AFTER_SEQUENCE_NUMBER,
        AT_TIMESTAMP,
        TRIM_HORIZON
    }
}

fn build_app() -> clap::App<'static, 'static> {
    let region = [
        Region::ApNortheast1,
        Region::ApNortheast2,
        Region::ApSouth1,
        Region::ApSoutheast1,
        Region::ApSoutheast2,
        Region::CaCentral1,
        Region::EuCentral1,
        Region::EuWest1,
        Region::EuWest2,
        Region::EuWest3,
        Region::SaEast1,
        Region::UsEast1,
        Region::UsEast2,
        Region::UsWest1,
        Region::UsWest2,
        Region::UsGovWest1,
        Region::CnNorth1,
        Region::CnNorthwest1,
    ].iter()
        .map(|x| x.name())
        .collect::<Vec<&str>>();
    App::new("k-iter")
        .about("AWS Kinesis Stream Subscriber")
        .version(crate_version!())
        .author(crate_authors!())
        .arg(
            Arg::with_name("stream-name")
                .short("n")
                .long("stream-name")
                .required(true)
                .value_name("NAME")
                .help("Sets a stream name.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("region")
                .short("r")
                .long("region")
                .required(true)
                .possible_values(&region)
                .value_name("NAME")
                .help("Sets a region name.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("shard-id")
                .short("s")
                .long("shard-id")
                .value_name("ID")
                .help("Sets shard id")
                .default_value("shardId-000000000000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("iterator-type")
                .short("t")
                .long("iterator-type")
                .possible_values(&IteratorType::variants())
                .requires_ifs(&[
                    ("AT_SEQUENCE_NUMBER", "sequence-number"),
                    ("AFTER_SEQUENCE_NUMBER", "sequence-number"),
                    ("AT_TIMESTAMP", "timestamp"),
                ])
                .default_value("LATEST")
                .value_name("TYPE")
                .help("Sets iterator type."),
        )
        .arg(
            Arg::with_name("sequence-number")
                .long("sequence-number")
                .value_name("NUM")
                .help("Set Sequence number when Iterator Type is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("timestamp")
                .long("timestamp")
                .value_name("TIMESTAMP")
                .help("Set timestamp(UNIX Epoch milliseconds) when Iterator Type is AT_TIMESTAMP.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("verbose")
                .long("verbose")
                .help("Enable verbose mode.")
                .default_value("false")
                .takes_value(false),
        )
}

fn records2string_only_data(records: &Vec<Record>) -> String {
    records.iter()
        .map(|x| String::from_utf8_lossy(&x.data).to_string())
        .collect::<Vec<String>>()
        .join("\n")
}

fn records2string_verbose(records: &Vec<Record>) -> String {
    records.iter()
        .filter_map(|x| serde_json::to_string(&RecordRef::new(x)).ok())
        .collect::<Vec<String>>()
        .join("\n")
}

fn main() {
    let matches = build_app().get_matches();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let name = value_t_or_exit!(matches.value_of("stream-name"), String);
    let id = value_t_or_exit!(matches.value_of("shard-id"), String);
    let region = value_t_or_exit!(matches.value_of("region"), Region);
    let iter_type: IteratorType = value_t_or_exit!(matches.value_of("iterator-type"), IteratorType);

    let printer = if matches.is_present("verbose") { records2string_verbose } else { records2string_only_data};

    let mut it = match iter_type {
        IteratorType::LATEST | IteratorType::TRIM_HORIZON => KinesisIterator::new(name, id, iter_type, region),
        IteratorType::AT_SEQUENCE_NUMBER | IteratorType::AFTER_SEQUENCE_NUMBER => {
            let seq = value_t_or_exit!(matches.value_of("sequence-number"), String);
            KinesisIterator::new_with_sequence_number(name, id, iter_type, seq, region)
        },
        IteratorType::AT_TIMESTAMP => {
            let timestamp = value_t_or_exit!(matches.value_of("timestamp"), f64);
            KinesisIterator::new_with_timestamp(name, id, iter_type, timestamp, region)
        },
    };

    while running.load(Ordering::SeqCst) {
        if let Some(Ok(n)) = it.next() {
            thread::sleep(time::Duration::from_millis(1000));
            if !n.records.is_empty() { println!("{}", printer(&n.records)); }
        }
    }
}