# cudl-python-copy

Dockerised AWS Lambda that syncs objects between S3 buckets in response to SQS-wrapped S3 event notifications.

## What it does

Triggered by an SQS queue that receives S3 event notifications from a source bucket.

| Event | Action |
|---|---|
| `ObjectCreated:*` | Copy object from source bucket to destination bucket, transforming the key |
| `ObjectRemoved:*` | Delete the corresponding object from the destination bucket |

Any other event types (e.g. `LifecycleExpiration`, replication events) are logged and ignored.

On `ObjectRemoved:*`, if the corresponding destination object does not exist, the delete is treated as a success rather than an error. This is intentional — it keeps retries idempotent and avoids failing a message when the destination is already in the desired state.

### Key transformation

`STRIP_PREFIX` is removed from the front of each key and `DEST_PREFIX` is prepended. Both default to empty, so with neither set the object is copied verbatim to the same key.

```
STRIP_PREFIX = "ui/"   DEST_PREFIX = "html/"

ui/cudl-resources/path/to/file/A  →  html/cudl-resources/path/to/file/A
```

With no prefixes set, the key passes through unchanged:

```
(no STRIP_PREFIX, no DEST_PREFIX)

ui/cudl-resources/path/to/file/A  →  ui/cudl-resources/path/to/file/A
```

Which objects are processed is decided by the S3 event notification filter (`filter_prefix` / `filter_suffix`), not by the handler.

> **Guardrail:** if `DEST_BUCKET` equals the source bucket, an identity copy re-triggers its own `ObjectCreated` notification → infinite loop. Keep the source and destination buckets distinct (as the data-source → data-releases flow already does).

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `DEST_BUCKET` | yes | — | Destination S3 bucket |
| `STRIP_PREFIX` | no | `""` | Prefix stripped off the front of the source key |
| `DEST_PREFIX` | no | `""` | Prefix prepended after stripping |
| `SOURCE_PREFIX` | no (**deprecated**) | — | Legacy alias for `STRIP_PREFIX`, honoured only when `STRIP_PREFIX` is unset. Logs a deprecation warning when used — prefer `STRIP_PREFIX` |
| `LOG_LEVEL` | no | `INFO` | Logging level (e.g. `DEBUG`, `INFO`, `WARNING`, `ERROR`). Unset or unrecognised falls back to `INFO` |

With none of the prefix variables set, objects are copied verbatim to the same key.

The source bucket is read from each S3 event notification (`s3.bucket.name`) rather than from configuration.

## Partial batch failure

The handler returns `batchItemFailures` so the SQS queue retries only failed messages. Configure the SQS event source mapping with `FunctionResponseTypes: [ReportBatchItemFailures]`.

## Local development

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Lint & format
ruff check . && black --check .

# Type check
mypy s3_replicator

# Run tests
pytest -v
```

## Build & deploy

Two Dockerfiles are provided. Use the one that matches your deployment target:

| File | Datadog | Use when |
|---|---|---|
| `Dockerfile` | No | Deployments without monitoring; local development |
| `Dockerfile.datadog` | Yes | Deployments with Datadog metrics, traces, and logs |

```bash
# Without Datadog
docker build -f Dockerfile -t cudl-python-copy .

# With Datadog
docker build -f Dockerfile.datadog -t cudl-python-copy-datadog .

# Tag and push to ECR (replace variables)
docker tag cudl-python-copy:latest $ECR_URI:latest
docker push $ECR_URI:latest
```

Then register the Lambda in the `cudl-data-processing` Terraform module (see below).

## Terraform configuration

This Lambda is deployed via the `cudl-data-processing` module using a Docker image URI.
Add an entry to `transform-lambda-information` in the relevant `terraform.tfvars`.

### Without Datadog (default)

```hcl
{
  name       = "cudl-python-copy"
  image_uri  = "<ecr-uri>:<tag>"
  timeout    = 60
  memory     = 256
  queue_name = "cudl-python-copy-queue"

  environment_variables = {
    DEST_BUCKET  = "<destination-bucket-name>"
    STRIP_PREFIX = "ui/"
    DEST_PREFIX  = "html/"
    # Omit STRIP_PREFIX and DEST_PREFIX for a verbatim same-path copy.
  }
}
```

### With Datadog

```hcl
{
  name       = "cudl-python-copy"
  image_uri  = "<ecr-uri>:<tag>"
  timeout    = 60
  memory     = 256
  queue_name = "cudl-python-copy-queue"

  use_datadog_variables = true
  datadog_runtime       = "python"
  command               = "datadog_lambda.handler.handler"

  environment_variables = {
    DEST_BUCKET       = "<destination-bucket-name>"
    STRIP_PREFIX      = "ui/"
    DEST_PREFIX       = "html/"
    DD_LAMBDA_HANDLER = "s3_replicator.handler.handler"
  }
}
```

`DD_LAMBDA_HANDLER` tells the Datadog wrapper where the real handler is.
The Datadog API key is resolved from AWS Secrets Manager at runtime via `DD_API_KEY_SECRET_ARN`
(set by the shared `lambda_environment_datadog_variables_python` variable) — it is never stored
in plaintext or Terraform state.

## IAM permissions required

The Lambda execution role needs:

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject"],
  "Resource": "arn:aws:s3:::<source-bucket>/*"
},
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:DeleteObject"],
  "Resource": "arn:aws:s3:::<DEST_BUCKET>/*"
}
```

Replace `<source-bucket>` with the actual source bucket name (or `*` if the queue may receive events from multiple source buckets).

## TODOs

- [ ] Add dead-letter queue (DLQ) for messages that exhaust SQS retries
- [ ] Structured JSON logging (e.g. `aws-lambda-powertools`) for better CloudWatch querying
