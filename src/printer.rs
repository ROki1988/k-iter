use kinesis_sdk::model::{EncryptionType, Record};
use serde_derive::Serialize;

use crate::cli::DataFormat;

#[derive(Debug, Clone, Serialize)]
struct RecordRef<'a, Data> {
    #[serde(rename = "ApproximateArrivalTimestamp")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approximate_arrival_timestamp: Option<f64>,
    #[serde(rename = "Data")]
    pub data: Data,
    #[serde(rename = "EncryptionType")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_type: &'a Option<EncryptionType>,
    #[serde(rename = "PartitionKey")]
    pub partition_key: &'a Option<String>,
    #[serde(rename = "SequenceNumber")]
    pub sequence_number: &'a Option<String>,
}

impl<'a, Data> RecordRef<'a, Data> {
    fn new_raw_array(origin: &'a Record) -> RecordRef<'_, Option<&[u8]>> {
        RecordRef {
            approximate_arrival_timestamp: origin
                .approximate_arrival_timestamp
                .map(|x| x.epoch_fractional_seconds()),
            data: origin.data.as_ref().map(|x| x.as_ref()),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }

    fn new_raw_string(origin: &'a Record) -> RecordRef<'_, String> {
        RecordRef {
            approximate_arrival_timestamp: origin
                .approximate_arrival_timestamp
                .map(|x| x.epoch_fractional_seconds()),
            data: origin
                .data
                .iter()
                .flat_map(|s| s.as_ref().iter().map(|x| format!("{:02x}", x)))
                .collect(),
            encryption_type: &origin.encryption_type,
            partition_key: &origin.partition_key,
            sequence_number: &origin.sequence_number,
        }
    }

    fn new_utf8_string(origin: &'a Record) -> RecordRef<'_, Option<&'a str>> {
        RecordRef {
            approximate_arrival_timestamp: origin
                .approximate_arrival_timestamp
                .map(|x| x.epoch_fractional_seconds()),
            data: origin
                .data
                .as_ref()
                .map(|x| std::str::from_utf8(&x.as_ref()).unwrap()),
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
        .filter_map(|r| r.data.as_ref())
        .map(|x| format!("{:?}", x.as_ref()))
        .collect::<Vec<String>>()
        .join("\n")
}

fn records2string_only_data_raw_string(records: &[Record]) -> String {
    records
        .iter()
        .filter_map(|x| x.data.as_ref())
        .map(|x| x.as_ref().iter().map(|&s| format!("{:02x}", s)).collect())
        .collect::<Vec<String>>()
        .join("\n")
}

fn records2string_only_data_utf8_string(records: &[Record]) -> String {
    records
        .iter()
        .flat_map(|x| x.data.as_ref())
        .filter_map(|x| std::str::from_utf8(x.as_ref()).ok())
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
    use super::*;
    use kinesis_sdk::model::Record;
    use kinesis_sdk::Blob;

    fn string_record() -> Record {
        kinesis_sdk::model::record::Builder::default()
            .data(Blob::new("test-data"))
            .partition_key("KEY")
            .sequence_number("1")
            .build()
    }

    fn bin_record() -> Record {
        kinesis_sdk::model::record::Builder::default()
            .data(Blob::new(vec![0u8, 255u8]))
            .partition_key("KEY")
            .sequence_number("1")
            .build()
    }

    #[test]
    fn test_only_data_utf8_string() {
        let records: Vec<Record> = vec![string_record()];

        assert_eq!(
            "test-data".to_owned(),
            records2string_only_data_utf8_string(records.as_slice())
        )
    }

    #[test]
    fn test_only_data_raw_byte() {
        let records: Vec<Record> = vec![bin_record()];

        assert_eq!(
            "[0, 255]".to_owned(),
            records2string_only_data_raw_byte(records.as_slice())
        )
    }

    #[test]
    fn test_only_data_raw_string() {
        let records: Vec<Record> = vec![bin_record()];

        assert_eq!(
            "00ff".to_owned(),
            records2string_only_data_raw_string(records.as_slice())
        )
    }

    #[test]
    fn test_verbose_utf8_string() {
        let records: Vec<Record> = vec![string_record()];

        assert_eq!(
            r#"{"Data":"test-data","PartitionKey":"KEY","SequenceNumber":"1"}"#.to_owned(),
            records2string_verbose_utf8_string(records.as_slice())
        )
    }

    #[test]
    fn test_verbose_raw_byte() {
        let records: Vec<Record> = vec![bin_record()];

        assert_eq!(
            r#"{"Data":[0,255],"PartitionKey":"KEY","SequenceNumber":"1"}"#.to_owned(),
            records2string_verbose_raw_byte(records.as_slice())
        )
    }

    #[test]
    fn test_verbose_raw_string() {
        let records: Vec<Record> = vec![bin_record()];

        assert_eq!(
            r#"{"Data":"00ff","PartitionKey":"KEY","SequenceNumber":"1"}"#.to_owned(),
            records2string_verbose_raw_string(records.as_slice())
        )
    }
}
