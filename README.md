# k-iter: Kinesis Subscriber for CLI

## Simple Command

```sh
k-iter -n stream -r ap-northeast-1
```

## Options

```sh
USAGE:
    k-iter.exe [OPTIONS] --region <NAME> --stream-name <NAME>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -r, --region <NAME>         Sets a region name. [possible values: ap-northeast-1, ap-northeast-2, ap-south-1, ap
                                -southeast-1, ap-southeast-2, ca-central-1, eu-central-1, eu-west-1, eu-west-2, eu
                                -west-3, sa-east-1, us-east-1, us-east-2, us-west-1, us-west-2, us-gov-west-1, cn
                                -north-1, cn-northwest-1]
    -s, --shard-id <ID>         Sets shard id [default: shardId-000000000000]
    -n, --stream-name <NAME>    Sets a stream name.
```

## Install

```
cargo install --git https://github.com/ROki1988/k-iter.git
```