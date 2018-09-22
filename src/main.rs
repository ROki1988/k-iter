#[macro_use]
extern crate clap;
extern crate ctrlc;
extern crate rusoto_core;
extern crate rusoto_kinesis;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use cli::{DataFormat, IteratorType};
use kinesis::KinesisShardIterator;
use rusoto_core::Region;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;

mod cli;
mod kinesis;
mod printer;

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
    let format_type: DataFormat = value_t_or_exit!(matches.value_of("data-format"), DataFormat);

    let printer = printer::RecordsPrinter::new(matches.is_present("verbose"), format_type);

    let mut it = match iter_type {
        IteratorType::LATEST | IteratorType::TRIM_HORIZON => {
            KinesisShardIterator::new(name, id, iter_type.to_string(), region)
        }
        IteratorType::AT_SEQUENCE_NUMBER | IteratorType::AFTER_SEQUENCE_NUMBER => {
            let seq = value_t_or_exit!(matches.value_of("sequence-number"), String);
            KinesisShardIterator::new_with_sequence_number(
                name,
                id,
                iter_type.to_string(),
                seq,
                region,
            )
        }
        IteratorType::AT_TIMESTAMP => {
            let timestamp = value_t_or_exit!(matches.value_of("timestamp"), f64);
            KinesisShardIterator::new_with_timestamp(
                name,
                id,
                iter_type.to_string(),
                timestamp,
                region,
            )
        }
    };

    while running.load(Ordering::SeqCst) {
        if let Some(Ok(n)) = it.next() {
            thread::sleep(time::Duration::from_millis(1000));
            if !n.records.is_empty() {
                println!("{}", printer.print(&n.records));
            }
        }
    }
}
