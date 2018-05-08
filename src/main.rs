#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rusoto_core;
extern crate rusoto_kinesis;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::process::{Command, Stdio};
use std::io::Write;
use std::thread;
use std::time;

use clap::{App, Arg};

use rusoto_core::Region;
use rusoto_kinesis::{Kinesis, KinesisClient, GetShardIteratorInput, GetRecordsInput, GetRecordsOutput, GetRecordsError, GetShardIteratorError, Record};

pub struct KinesisIterator {
    client: KinesisClient,
    input: GetShardIteratorInput,
    token: Option<String>,
}

impl KinesisIterator {
    pub fn new(stream_name: String, shard_id: String, shard_iterator_type: String, region: Region) -> Self {
        let c = KinesisClient::simple(region);
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type,
            stream_name,
            ..Default::default()
        };
        KinesisIterator {
            client: c,
            input,
            token: None,
        }
    }

    pub fn get_iterator_token(&self) -> Result<Option<String>, GetShardIteratorError> {
        self.client
            .get_shard_iterator(&self.input).sync()
            .map(|x| x.shard_iterator)
    }
}

impl Iterator for KinesisIterator {
    type Item = Result<Vec<Vec<u8>>, GetRecordsError>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.token.clone()
            .or_else(|| self.get_iterator_token().unwrap())
            .map(|x| {
                self.token = Some(x.clone());
                let r = GetRecordsInput {
                    shard_iterator: x,
                    ..Default::default()
                };
                self.client.get_records(&r).sync()
                    .map(|x| {
                        self.token = x.next_shard_iterator.clone();
                        x.records.into_iter().map(|r| r.data).collect()
                    })
            })

    }
}

arg_enum! {
    #[derive(Debug)]
    enum IteratorType {
        LATEST,
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
    ].iter().map(|x| x.name()).collect::<Vec<&str>>();
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
                .takes_value(true)
        )
        .arg(
            Arg::with_name("region")
                .short("r")
                .long("region")
                .required(true)
                .possible_values(&region)
                .value_name("NAME")
                .help("Sets a region name.")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("shard-id")
                .short("s")
                .long("shard-id")
                .value_name("ID")
                .help("Sets shard id")
                .default_value("shardId-000000000000")
                .takes_value(true)
        )
        .arg(
            Arg::with_name("iterator-type")
                .short("t")
                .long("iterator-type")
                .possible_values(&IteratorType::variants())
                .default_value("LATEST")
                .value_name("TYPE")
                .help("Sets iterator type.")
        )
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
    let iter_type = value_t_or_exit!(matches.value_of("iterator-type"), String);

    let mut it = KinesisIterator::new(name, id, iter_type, region);

    while running.load(Ordering::SeqCst) {
        if let Some(Ok(n)) = it.next() {
            thread::sleep(time::Duration::from_millis(1000));
            n.iter()
                .for_each(|x| println!("{}", String::from_utf8_lossy(x)));
        }
    }
}