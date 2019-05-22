use rusoto_core::Region;
use rusoto_core::RusotoError;
use rusoto_kinesis::{
    GetRecordsError, GetRecordsInput, GetRecordsOutput, GetShardIteratorError,
    GetShardIteratorInput, Kinesis, KinesisClient,
};
use futures::{Future, Stream, Poll};

pub struct KinesisIterator {
    client: KinesisClient,
    input: GetShardIteratorInput,
    token: Option<String>,
}

impl KinesisIterator {
    fn new_self(input: GetShardIteratorInput, region: Region) -> Self {
        let c = KinesisClient::new(region);
        KinesisIterator {
            client: c,
            input,
            token: None,
        }
    }

    pub fn new(
        stream_name: String,
        shard_id: String,
        shard_iterator_type: String,
        region: Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type,
            stream_name,
            ..Default::default()
        };
        KinesisIterator::new_self(input, region)
    }

    pub fn new_with_sequence_number(
        stream_name: String,
        shard_id: String,
        shard_iterator_type: String,
        sequence_number: String,
        region: Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type,
            stream_name,
            starting_sequence_number: Some(sequence_number),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region)
    }

    pub fn new_with_timestamp(
        stream_name: String,
        shard_id: String,
        shard_iterator_type: String,
        timestamp: f64,
        region: Region,
    ) -> Self {
        let input = GetShardIteratorInput {
            shard_id,
            shard_iterator_type,
            stream_name,
            timestamp: Some(timestamp),
            ..Default::default()
        };
        KinesisIterator::new_self(input, region)
    }

    pub fn get_iterator_token(&self) -> Result<Option<String>, RusotoError<GetShardIteratorError>> {
        self.client
            .get_shard_iterator(self.input.clone())
            .sync()
            .map(|x| x.shard_iterator)
    }
}

impl Iterator for KinesisIterator {
    type Item = Result<GetRecordsOutput, RusotoError<GetRecordsError>>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.token
            .clone()
            .or_else(|| self.get_iterator_token().unwrap())
            .map(|x| {
                self.token = Some(x.clone());
                let r = GetRecordsInput {
                    shard_iterator: x,
                    ..Default::default()
                };
                self.client.get_records(r).sync().map(|x| {
                    self.token = x.next_shard_iterator.clone();
                    x
                })
            })
    }
}

impl Stream for KinesisIterator {
    type Item = GetRecordsOutput;
    type Error = RusotoError<GetRecordsError>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.token
            .clone()
            .or_else(|| self.get_iterator_token().unwrap())
            .map(|x| {
                self.token = Some(x.clone());
                let r = GetRecordsInput {
                    shard_iterator: x,
                    ..Default::default()
                };
                self.client.get_records(r).map(|x| {
                    self.token = x.next_shard_iterator.clone();
                    x
                })
            }).poll()
    }
}