use crate::error::{Error, ErrorKind};
use async_stream::try_stream;
use futures::{Stream, TryFutureExt};
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
    pub async fn get_shard_ids(name: &str, region: &Region) -> Result<Vec<Shard>, Error> {
        let c = KinesisClient::new(region.clone());
        c.list_shards(ListShardsInput {
            stream_name: Some(name.to_string()),
            ..Default::default()
        })
        .await
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

    pub async fn get_iterator_token(&self) -> Result<String, Error> {
        self.client
            .get_shard_iterator(self.input.clone())
            .await
            .map_err(Into::into)
            .and_then(|x| x.shard_iterator.ok_or_else(|| Error::from(ErrorKind::Rusoto)))
    }

    pub async fn get_records(&self, token: &str) -> Result<GetRecordsOutput, Error> {
        let r = GetRecordsInput {
            shard_iterator: token.to_owned(),
            ..Default::default()
        };

        self.client.get_records(r).map_err(Into::into).await
    }

    pub fn stream(mut self) -> impl Stream<Item = Result<GetRecordsOutput, Error>> {
        try_stream! {
            loop {
                if let Some(current) = &self.token {
                    let r = self.get_records(current).await?;
                    self.token = r.next_shard_iterator.clone();
                    yield r;
                } else {
                    let next = self.get_iterator_token().await?;
                    self.token = Some(next);
                    continue;
                }
            }
        }
    }
}
