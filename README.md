# AsynQ

## Build

```sh
$ go build
```

## Usage

```sh
$ AWS_ACCESS_KEY_ID="YOUR_AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="YOUR_AWS_SECRET_ACCESS_KEY" \
  S3_BUCKET="YOUR_S3_BUCKET" \
  S3_ENDPOINT="YOUR_S3_ENDPOINT" \
  S3_REGION="YOUR_S3_REGION" \
      ./asynq
```

For test, [MinIO](https://github.com/minio/minio) is handy:

```sh
$ AWS_ACCESS_KEY_ID="minioadmin" \
  AWS_SECRET_ACCESS_KEY="minioadmin" \
  S3_BUCKET="asynq-test" \
  S3_ENDPOINT="http://127.0.0.1:9000" \
  S3_REGION="foo" \
      ./asynq
```
