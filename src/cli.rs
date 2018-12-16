use clap::{_clap_count_exprs, App, Arg, arg_enum, crate_authors, crate_version};
use rusoto_core::Region;

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum IteratorType {
        LATEST,
        AT_SEQUENCE_NUMBER,
        AFTER_SEQUENCE_NUMBER,
        AT_TIMESTAMP,
        TRIM_HORIZON,
    }
}

arg_enum! {
    #[derive(PartialEq, Debug)]
    pub enum DataFormat {
        RAW_BYTES,
        RAW_STRING,
        UTF8_STRING,
    }
}

pub fn build_app() -> App<'static, 'static> {
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
                .help("Set shard id.")
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
        .arg(
            Arg::with_name("data-format")
                .long("data-format")
                .possible_values(&DataFormat::variants())
                .value_name("TYPE")
                .help("Set data output-format.")
                .default_value("UTF8_STRING")
                .takes_value(true),
        )
}
