# Source to point the engine at the proxied RustFS (+latency) endpoint.
export AWS_ACCESS_KEY_ID=omnigraphci
export AWS_SECRET_ACCESS_KEY=omnigraphci-secret
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://127.0.0.1:${S3_PORT:-19000}
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:${S3_PORT:-19000}
export AWS_ALLOW_HTTP=true
export AWS_S3_FORCE_PATH_STYLE=true
export OG_CW_TARGET=s3://omnigraph-bench/cw
