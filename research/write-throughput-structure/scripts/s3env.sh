# Source me: S3 env for RustFS behind toxiproxy (+15 ms each way) on 127.0.0.1:19000.
export AWS_ACCESS_KEY_ID=omnigraphci AWS_SECRET_ACCESS_KEY=omnigraphci-secret AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://127.0.0.1:19000 AWS_ENDPOINT_URL_S3=http://127.0.0.1:19000
export AWS_ALLOW_HTTP=true AWS_S3_FORCE_PATH_STYLE=true
export OMNIGRAPH_S3_TEST_BUCKET=omnigraph-bench
