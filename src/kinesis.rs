use anyhow::Result;
use async_stream::try_stream;
use futures::{Stream, TryFutureExt};
use kinesis_sdk::input::GetShardIteratorInput;
use kinesis_sdk::model::{Shard, ShardIteratorType};
use kinesis_sdk::output::GetRecordsOutput;
use kinesis_sdk::Client as KinesisClient;
use kinesis_sdk::Region;

pub struct KinesisShardIterator {
    client: KinesisClient,
    input: GetShardIteratorInput,
    token: Option<String>,
}

impl KinesisShardIterator {
    pub async fn get_shard_ids(name: &str, region: &Region) -> Result<Vec<Shard>> {
        let c = KinesisClient::from_env();
        c.list_shards()
            .stream_name(name)
            .send()
            .await
            .map(|xs| xs.shards.unwrap())
            .map_err(Into::into)
    }

    pub fn new(
        stream_name: &str,
        shard_id: &str,
        shard_iterator_type: &str,
        region: &Region,
    ) -> Self {
        let client = KinesisClient::from_env();
        let input = GetShardIteratorInput::builder()
            .stream_name(stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::from(shard_iterator_type))
            .build()
            .expect("Can't create GetShardIteratorInput");
        Self {
            client,
            input,
            token: None,
        }
    }

    pub fn new_with_sequence_number(
        stream_name: &str,
        shard_id: &str,
        shard_iterator_type: &str,
        sequence_number: &str,
        region: &Region,
    ) -> Self {
        let client = KinesisClient::from_env();
        let input = GetShardIteratorInput::builder()
            .stream_name(stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::from(shard_iterator_type))
            .starting_sequence_number(sequence_number)
            .build()
            .expect("Can't create GetShardIteratorInput");
        Self {
            client,
            input,
            token: None,
        }
    }

    pub fn new_with_timestamp(
        stream_name: &str,
        shard_id: &str,
        shard_iterator_type: &str,
        timestamp: f64,
        region: &Region,
    ) -> Self {
        let client = KinesisClient::from_env();
        let input = GetShardIteratorInput::builder()
            .stream_name(stream_name)
            .shard_id(shard_id)
            .shard_iterator_type(ShardIteratorType::from(shard_iterator_type))
            .timestamp(smithy_types::Instant::from_f64(timestamp))
            .build()
            .expect("Can't create GetShardIteratorInput");
        Self {
            client,
            input,
            token: None,
        }
    }

    pub async fn get_iterator_token(&self) -> Result<String> {
        self.client
            .get_shard_iterator()
            .set_stream_name(self.input.stream_name.clone())
            .set_shard_id(self.input.shard_id.clone())
            .set_shard_iterator_type(self.input.shard_iterator_type.clone())
            .set_starting_sequence_number(self.input.starting_sequence_number.clone())
            .set_timestamp(self.input.timestamp)
            .send()
            .await
            .map_err(Into::into)
            .and_then(|x| {
                x.shard_iterator
                    .ok_or_else(|| anyhow::anyhow!("None iterator token"))
            })
    }

    async fn get_records(&self, token: &str) -> Result<GetRecordsOutput> {
        self.client
            .get_records()
            .shard_iterator(token)
            .send()
            .map_err(Into::into)
            .await
    }

    pub fn stream(mut self) -> impl Stream<Item = Result<GetRecordsOutput>> {
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
