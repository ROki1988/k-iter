use futures::{Future, Poll, Stream, Async};
use rusoto_core::Region;
use rusoto_core::RusotoError;
use rusoto_kinesis::{GetRecordsError, GetRecordsInput, GetRecordsOutput, GetShardIteratorError, GetShardIteratorInput, Kinesis, KinesisClient, Record, ListShardsInput, Shard, ListShardsError};
use std::time::Duration;

pub struct KinesisIterator {
    client: KinesisClient,
    input: GetShardIteratorInput,
    token: Option<String>,
}

impl KinesisIterator {
    pub fn get_shard_ids(name: &str, region: &Region) -> Result<Vec<Shard>, RusotoError<ListShardsError>> {
        let c = KinesisClient::new(region.clone());
        c.list_shards(ListShardsInput{stream_name: Some(name.to_string()), ..Default::default()}).sync()
            .map(|xs| xs.shards.unwrap())
    }

    fn new_self(input: GetShardIteratorInput, region: Region) -> Self {
        let c = KinesisClient::new(region);
        KinesisIterator {
            client: c,
            input,
            token: None,
        }
    }

    pub fn new(
        stream_name: & str,
        shard_id: & str,
        shard_iterator_type: & str,
        region: &Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id: shard_id.to_string(),
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name: stream_name.to_string(),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region.clone())
    }

    pub fn new_with_sequence_number(
        stream_name: & str,
        shard_id: & str,
        shard_iterator_type: & str,
        sequence_number: & str,
        region: &Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id: shard_id.to_string(),
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name: stream_name.to_string(),
            starting_sequence_number: Some(sequence_number.to_string()),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region.clone())
    }

    pub fn new_with_timestamp(
        stream_name: & str,
        shard_id: & str,
        shard_iterator_type: & str,
        timestamp: f64,
        region: &Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id: shard_id.to_string(),
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name: stream_name.to_string(),
            timestamp: Some(timestamp),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region.clone())
    }

    pub fn get_iterator_token(&self) -> Result<String, RusotoError<GetShardIteratorError>> {
        self.client
            .get_shard_iterator(self.input.clone())
            .sync()
            .map(|x| x.shard_iterator.unwrap())
    }
}

impl Stream for KinesisIterator {
    type Item = GetRecordsOutput;
    type Error = RusotoError<GetRecordsError>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        futures::future::ok(self.token
            .clone()
            .unwrap_or(self.get_iterator_token().unwrap()))
            .and_then(|x| {
                self.token = Some(x.clone());
                let r = GetRecordsInput {
                    shard_iterator: x,
                    ..Default::default()
                };

                self.client.get_records(r).map(|r   | {
                    self.token = r.next_shard_iterator.clone();
                    Async::Ready(Some(r))
                })
            }).wait()
    }
}
