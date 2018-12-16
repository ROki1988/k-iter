use std;

use rusoto_kinesis::Record;
use serde_derive::Serialize;
use serde_json;

use crate::cli::DataFormat;

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
    fn new_raw_array(origin: &'a Record) -> RecordRef<'_, &[u8]> {
        RecordRef {
            approximate_arrival_timestamp: &origin.approximate_arrival_timestamp,
            data: origin.data.as_slice(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }

    fn new_raw_string(origin: &'a Record) -> RecordRef<'_, String> {
        RecordRef {
            approximate_arrival_timestamp: &origin.approximate_arrival_timestamp,
            data: origin.data.iter().map(|&s| format!("{:02x}", s)).collect(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }

    fn new_utf8_string(origin: &'a Record) -> RecordRef<'_, &str> {
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
                    DataFormat::RAW_BYTES => records2string_verbose_raw_byte,
                    DataFormat::RAW_STRING => records2string_verbose_raw_string,
                    DataFormat::UTF8_STRING => records2string_verbose_utf8_string,
                }
            } else {
                match data_format {
                    DataFormat::RAW_BYTES => records2string_only_data_raw_byte,
                    DataFormat::RAW_STRING => records2string_only_data_raw_string,
                    DataFormat::UTF8_STRING => records2string_only_data_utf8_string,
                }
            },
        }
    }

    pub fn print(&self, records: &[Record]) -> String {
        (self.printer)(records)
    }
}

fn records2string_only_data_raw_byte(records: &[Record]) -> String {
    records
        .iter()
        .map(|x| format!("{:?}", x.data.as_slice()))
        .collect::<Vec<String>>()
        .join("\n")
}

fn records2string_only_data_raw_string(records: &[Record]) -> String {
    records
        .iter()
        .map(|x| x.data.iter().map(|&s| format!("{:02x}", s)).collect())
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

#[cfg(test)]
mod tests {
    use rusoto_kinesis::Record;

    use super::*;

    #[test]
    fn test_only_data_utf8_string() {
        let records: Vec<Record> = vec![Record {
            approximate_arrival_timestamp: None,
            data: "test-data".to_owned().into_bytes(),
            encryption_type: None,
            partition_key: "KEY".to_owned(),
            sequence_number: "1".to_owned(),
        }];

        assert_eq!(
            "test-data".to_owned(),
            records2string_only_data_utf8_string(records.as_slice())
        )
    }

    #[test]
    fn test_only_data_raw_byte() {
        let records: Vec<Record> = vec![Record {
            approximate_arrival_timestamp: None,
            data: vec![0u8, 255u8],
            encryption_type: None,
            partition_key: "KEY".to_owned(),
            sequence_number: "1".to_owned(),
        }];

        assert_eq!(
            "[0, 255]".to_owned(),
            records2string_only_data_raw_byte(records.as_slice())
        )
    }

    #[test]
    fn test_only_data_raw_string() {
        let records: Vec<Record> = vec![Record {
            approximate_arrival_timestamp: None,
            data: vec![0u8, 255u8],
            encryption_type: None,
            partition_key: "KEY".to_owned(),
            sequence_number: "1".to_owned(),
        }];

        assert_eq!(
            "00ff".to_owned(),
            records2string_only_data_raw_string(records.as_slice())
        )
    }

    #[test]
    fn test_verbose_utf8_string() {
        let records: Vec<Record> = vec![Record {
            approximate_arrival_timestamp: None,
            data: "test-data".to_owned().into_bytes(),
            encryption_type: None,
            partition_key: "KEY".to_owned(),
            sequence_number: "1".to_owned(),
        }];

        assert_eq!(
            r#"{"Data":"test-data","PartitionKey":"KEY","SequenceNumber":"1"}"#.to_owned(),
            records2string_verbose_utf8_string(records.as_slice())
        )
    }

    #[test]
    fn test_verbose_raw_byte() {
        let records: Vec<Record> = vec![Record {
            approximate_arrival_timestamp: None,
            data: vec![0u8, 255u8],
            encryption_type: None,
            partition_key: "KEY".to_owned(),
            sequence_number: "1".to_owned(),
        }];

        assert_eq!(
            r#"{"Data":[0,255],"PartitionKey":"KEY","SequenceNumber":"1"}"#.to_owned(),
            records2string_verbose_raw_byte(records.as_slice())
        )
    }

    #[test]
    fn test_verbose_raw_string() {
        let records: Vec<Record> = vec![Record {
            approximate_arrival_timestamp: None,
            data: vec![0u8, 255u8],
            encryption_type: None,
            partition_key: "KEY".to_owned(),
            sequence_number: "1".to_owned(),
        }];

        assert_eq!(
            r#"{"Data":"00ff","PartitionKey":"KEY","SequenceNumber":"1"}"#.to_owned(),
            records2string_verbose_raw_string(records.as_slice())
        )
    }
}
