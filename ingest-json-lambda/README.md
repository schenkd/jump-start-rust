# ingest-json-lambda

The purpose of the application is to ingest json data coming from Kinesis and sink them on S3 as delta table.  

Further readings:
* [Using AWS Lambda with Amazon Kinesis](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html)
* [Getting started with deltalake](https://docs.delta.io/latest/delta-intro.html)


## Testing

```bash
cargo test
```

If you want to see your print statements during test execution
```bash
cargo test -- --nocapture
```

## Build

```bash
cargo lambda build --release --arm64
```

In case you receive an build error regarding missing ssl lib on macos
```bash
export OPENSSL_DIR=/usr/local/opt/openssl/
cargo lambda build --release --arm64
```

## Deploy

You should prepare your AWS account with a IAM role that can be assumed by your lambda function.

```bash
cargo lambda deploy ingest-json-lambda
```