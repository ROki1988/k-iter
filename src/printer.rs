use cli::DataFormat;
use rusoto_kinesis::Record;
use serde_json;
use std;

#[derive(Debug, Clone, Serialize)]
struct RecordRef<'a, Data> {
    #[serde(rename = "ApproximateArrivalTimestamp")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approximate_arrival_timestamp: &'a Option<f64>,
    #[serde(rename = "Data")]
    pub data: Data,
    #[serde(rename = "EncryptionType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_type: &'a Option<String>,
    #[serde(rename = "PartitionKey")]
    pub partition_key: &'a String,
    #[serde(rename = "SequenceNumber")]
    pub sequence_number: &'a String,
}

impl<'a, Data> RecordRef<'a, Data> {
    fn new_raw_array(origin: &'a Record) -> RecordRef<&[u8]> {
        RecordRef {
            approximate_arrival_timestamp: &origin.approximate_arrival_timestamp,
            data: origin.data.as_slice(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }

    fn new_raw_string(origin: &'a Record) -> RecordRef<String> {
        RecordRef {
            approximate_arrival_timestamp: &origin.approximate_arrival_timestamp,
            data: origin.data.iter().map(|&s| s as char).collect(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }

    fn new_utf8_string(origin: &'a Record) -> RecordRef<&str> {
        RecordRef {
            approximate_arrival_timestamp: &origin.approximate_arrival_timestamp,
            data: std::str::from_utf8(&origin.data).unwrap(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }
}

pub struct RecordsPrinter {
    printer: fn(&[Record]) -> String,
}

impl RecordsPrinter {
    pub fn new(is_verbose: bool, data_format: DataFormat) -> Self {
        Self {
            printer: if is_verbose {
                match data_format {
                    DataFormat::RAW_BYTES => Self::records2string_verbose_raw_byte,
                    DataFormat::RAW_STRING => Self::records2string_verbose_raw_string,
                    DataFormat::UTF8_STRING => Self::records2string_verbose_utf8_string,
                }
            } else {
                match data_format {
                    DataFormat::RAW_BYTES => Self::records2string_only_data_raw_byte,
                    DataFormat::RAW_STRING => Self::records2string_only_data_raw_string,
                    DataFormat::UTF8_STRING => Self::records2string_only_data_utf8_string,
                }
            },
        }
    }

    pub fn print(&self, records: &[Record]) -> String {
        (self.printer)(records)
    }

    fn records2string_only_data_raw_byte(records: &[Record]) -> String {
        records
            .iter()
            .filter_map(|x| serde_json::to_string(&x.data).ok())
            .collect::<Vec<String>>()
            .join("\n")
    }

    fn records2string_only_data_raw_string(records: &[Record]) -> String {
        records
            .iter()
            .map(|x| x.data.iter().map(|&s| s as char).collect())
            .collect::<Vec<String>>()
            .join("\n")
    }

    fn records2string_only_data_utf8_string(records: &[Record]) -> String {
        records
            .iter()
            .filter_map(|x| std::str::from_utf8(&x.data).ok())
            .collect::<Vec<&str>>()
            .join("\n")
    }

    fn records2string_verbose_raw_byte(records: &[Record]) -> String {
        records
            .iter()
            .filter_map(|x| serde_json::to_string(&RecordRef::<&[u8]>::new_raw_array(x)).ok())
            .collect::<Vec<String>>()
            .join("\n")
    }

    fn records2string_verbose_raw_string(records: &[Record]) -> String {
        records
            .iter()
            .filter_map(|x| serde_json::to_string(&RecordRef::<String>::new_raw_string(x)).ok())
            .collect::<Vec<String>>()
            .join("\n")
    }

    fn records2string_verbose_utf8_string(records: &[Record]) -> String {
        records
            .iter()
            .filter_map(|x| serde_json::to_string(&RecordRef::<&str>::new_utf8_string(x)).ok())
            .collect::<Vec<String>>()
            .join("\n")
    }
}
