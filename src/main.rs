use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::{value_t_or_exit, values_t};
use ctrlc;
use rusoto_core::Region;
use tokio::timer::Interval;

use crate::cli::{DataFormat, IteratorType};
use crate::kinesis::KinesisShardIterator;
use futures::future::lazy;
use futures::future::Future;
use futures::Stream;
use rusoto_kinesis::GetRecordsOutput;
use std::time::Duration;
use futures::sink::Sink;

mod cli;
mod error;
mod kinesis;
mod printer;

fn main() {
    let matches = cli::build_app().get_matches();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let name = value_t_or_exit!(matches.value_of("stream-name"), String);
    let region = value_t_or_exit!(matches.value_of("region"), Region);
    let iter_type: IteratorType = value_t_or_exit!(matches.value_of("iterator-type"), IteratorType);
    let format_type: DataFormat = value_t_or_exit!(matches.value_of("data-format"), DataFormat);
    let ids: Option<Vec<String>> = values_t!(matches.values_of("shard-id"), String).ok();

    let printer = printer::RecordsPrinter::new(matches.is_present("verbose"), format_type);

    let shards = ids.unwrap_or_else(|| {
        KinesisShardIterator::get_shard_ids(name.as_str(), &region)
            .expect("can't get shard ids")
            .into_iter()
            .map(|s| s.shard_id)
            .collect()
    });

    tokio::run(lazy(move || {
        let (tx, rx) = tokio::sync::mpsc::channel::<GetRecordsOutput>(1000 * shards.len());
        let na = name.as_str();
        let ra = &region;
        let ta = iter_type.to_string();
        let ta = ta.as_str();

        for ia in shards.iter().map(String::as_str) {
            let it = match iter_type {
                IteratorType::LATEST | IteratorType::TRIM_HORIZON => {
                    KinesisShardIterator::new(na, ia, ta, ra)
                }
                IteratorType::AT_SEQUENCE_NUMBER | IteratorType::AFTER_SEQUENCE_NUMBER => {
                    let seq = value_t_or_exit!(matches.value_of("sequence-number"), String);
                    KinesisShardIterator::new_with_sequence_number(na, ia, ta, seq.as_str(), ra)
                }
                IteratorType::AT_TIMESTAMP => {
                    let timestamp = value_t_or_exit!(matches.value_of("timestamp"), f64);
                    KinesisShardIterator::new_with_timestamp(na, ia, ta, timestamp, ra)
                }
            };

            tokio::spawn({
                Interval::new_interval(Duration::from_millis(1000))
                    .map_err(|e| eprintln!("timer failed; err={:?}", e))
                    .zip(it.map_err(|e| eprintln!("subscribe error = err{:?}", e)))
                    .map(|(_, r)| r)
                    .forward(tx.clone().sink_map_err(|e| eprintln!("send error = err{:?}", e)))
                    .and_then(|_| Ok(()))

            });
        }

        rx.for_each(move |value| {
            if !value.records.is_empty() {
                println!("{}", printer.print(value.records.as_slice()));
            }
            Ok(())
        })
        .map_err(|e| eprintln!("receive error = err{:?}", e))
    }));
}
