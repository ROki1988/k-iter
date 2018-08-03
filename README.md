# k-iter: Kinesis Subscriber for CLI

[![Build Status](https://travis-ci.org/ROki1988/k-iter.svg?branch=master)](https://travis-ci.org/ROki1988/k-iter) [![Build status](https://ci.appveyor.com/api/projects/status/omrdwsvjciq68xf9?svg=true)](https://ci.appveyor.com/project/ROki1988/k-iter) [![dependency status](https://deps.rs/repo/github/ROki1988/k-iter/status.svg)](https://deps.rs/repo/github/ROki1988/k-iter)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FROki1988%2Fk-iter.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2FROki1988%2Fk-iter?ref=badge_shield)

## Simple Command

```sh
k-iter -n stream -r ap-northeast-1
```

## Options

```bash
USAGE:
    k-iter.exe [FLAGS] [OPTIONS] --region <NAME> --stream-name <NAME>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
        --verbose    Enable verbose mode.

OPTIONS:
        --data-format <TYPE>       Set data output-format. [default: UTF8_STRING]  [possible values: RAW_BYTES,
                                   RAW_STRING, UTF8_STRING]
    -t, --iterator-type <TYPE>     Sets iterator type. [default: LATEST]  [possible values: LATEST, AT_SEQUENCE_NUMBER,
                                   AFTER_SEQUENCE_NUMBER, AT_TIMESTAMP, TRIM_HORIZON]
    -r, --region <NAME>            Sets a region name. [possible values: ap-northeast-1, ap-northeast-2, ap-south-1, ap
                                   -southeast-1, ap-southeast-2, ca-central-1, eu-central-1, eu-west-1, eu-west-2, eu
                                   -west-3, sa-east-1, us-east-1, us-east-2, us-west-1, us-west-2, us-gov-west-1, cn
                                   -north-1, cn-northwest-1]
        --sequence-number <NUM>    Set Sequence number when Iterator Type is AT_SEQUENCE_NUMBER or
                                   AFTER_SEQUENCE_NUMBER.
    -s, --shard-id <ID>            Set shard id. [default: shardId-000000000000]
    -n, --stream-name <NAME>       Sets a stream name.
        --timestamp <TIMESTAMP>    Set timestamp(UNIX Epoch milliseconds) when Iterator Type is AT_TIMESTAMP.
```

## Install

```
cargo install --git https://github.com/ROki1988/k-iter.git
```

## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FROki1988%2Fk-iter.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2FROki1988%2Fk-iter?ref=badge_large)