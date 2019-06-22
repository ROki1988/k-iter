use crate::error::{Error, ErrorKind};
use futures::future::*;
use futures::{Async, Future, Poll, Stream};
use rusoto_core::Region;
use rusoto_kinesis::{
    GetRecordsInput, GetRecordsOutput, GetShardIteratorInput, Kinesis, KinesisClient,
    ListShardsInput, Shard,
};

pub struct KinesisShardIterator {
    client: KinesisClient,
    input: GetShardIteratorInput,
    token: Option<String>,
}

impl KinesisShardIterator {
    pub fn get_shard_ids(name: &str, region: &Region) -> Result<Vec<Shard>, Error> {
        let c = KinesisClient::new(region.clone());
        c.list_shards(ListShardsInput {
            stream_name: Some(name.to_string()),
            ..Default::default()
        })
        .sync()
        .map(|xs| xs.shards.unwrap())
        .map_err(Into::into)
    }

    fn new_self(input: GetShardIteratorInput, region: Region) -> Self {
        let c = KinesisClient::new(region);
        KinesisShardIterator {
            client: c,
            input,
            token: None,
        }
    }

    pub fn new(
        stream_name: &str,
        shard_id: &str,
        shard_iterator_type: &str,
        region: &Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id: shard_id.to_string(),
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name: stream_name.to_string(),
            ..Default::default()
        };
        KinesisShardIterator::new_self(input, region.clone())
    }

    pub fn new_with_sequence_number(
        stream_name: &str,
        shard_id: &str,
        shard_iterator_type: &str,
        sequence_number: &str,
        region: &Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id: shard_id.to_string(),
            shard_iterator_type: shard_iterator_type.to_string(),
            stream_name: stream_name.to_string(),
            starting_sequence_number: Some(sequence_number.to_string()),
            ..Default::default()
        };
        KinesisShardIterator::new_self(input, region.clone())
    }

    pub fn new_with_timestamp(
        stream_name: &str,
        shard_id: &str,
        shard_iterator_type: &str,
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
        KinesisShardIterator::new_self(input, region.clone())
    }

    pub fn get_iterator_token(&self) -> Result<String, Error> {
        self.client
            .get_shard_iterator(self.input.clone())
            .sync()
            .map_err(Into::into)
            .and_then(|x| {
                x.shard_iterator
                    .map_or_else(|| Err(Error::from(ErrorKind::Rusoto)), |token| Ok(token))
            })
    }
}

impl Stream for KinesisShardIterator {
    type Item = GetRecordsOutput;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(current) = &self.token {
            let r = GetRecordsInput {
                shard_iterator: current.clone(),
                ..Default::default()
            };

            self.client
                .get_records(r)
                .map(|r| {
                    self.token = r.next_shard_iterator.clone();
                    Async::Ready(Some(r))
                })
                .map_err(Into::into)
                .wait()
        } else {
            self.get_iterator_token()
                .map(|next| {
                    self.token = Some(next);
                    Async::NotReady
                })
                .map_err(Into::into)
        }
    }
}
