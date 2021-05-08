use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::{value_t_or_exit, values_t};
use kinesis_sdk::Region;
use tokio::time;

use crate::cli::{DataFormat, IteratorType};
use crate::kinesis::KinesisShardIterator;

use futures::sink::SinkExt;
use futures::{self, StreamExt, TryStreamExt};
use kinesis_sdk::output::GetRecordsOutput;
use std::option::Option::Some;
use std::process::exit;
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;

mod cli;
mod kinesis;
mod printer;

#[tokio::main]
async fn main() {
    let matches = cli::build_app().get_matches();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");

    let name = value_t_or_exit!(matches.value_of("stream-name"), String);
    let region: Region = Region::new(value_t_or_exit!(matches.value_of("region"), String));
    let iter_type: IteratorType = value_t_or_exit!(matches.value_of("iterator-type"), IteratorType);
    let format_type: DataFormat = value_t_or_exit!(matches.value_of("data-format"), DataFormat);
    let ids: Option<Vec<String>> = values_t!(matches.values_of("shard-id"), String).ok();

    let printer = printer::RecordsPrinter::new(matches.is_present("verbose"), format_type);

    let shards = if let Some(i) = ids {
        i
    } else {
        KinesisShardIterator::get_shard_ids(name.as_str(), &region)
            .await
            .expect("can't get shard ids")
            .into_iter()
            .flat_map(|s| s.shard_id)
            .collect()
    };

    let (tx, rx) = futures::channel::mpsc::channel::<GetRecordsOutput>(1000 * shards.len());
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

        let t = tx.clone();
        tokio::spawn(async move {
            IntervalStream::new(time::interval(Duration::from_millis(1000)))
                .zip(
                    it.stream()
                        .map_err(|e| eprintln!("subscribe error = err{:?}", e)),
                )
                .map(|(_, r)| r)
                .forward(t.sink_map_err(|e| eprintln!("send error = err{:?}", e)))
                .await
        });
    }

    rx.for_each(|value| {
        if !running.load(Ordering::SeqCst) {
            exit(0)
        };
        if let Some(records) = value.records {
            println!("{}", printer.print(records.as_slice()));
        }
        futures::future::ready(())
    })
    .await
}
