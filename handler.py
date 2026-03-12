"""Lambda handler: copies/deletes S3 objects in response to SQS-wrapped S3 events."""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_s3: Any = None  # module-level client, replaced in tests


def _get_s3() -> Any:
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


def transform_key(key: str, guard_prefix: str, strip_prefix: str, dest_prefix: str) -> str:
    """Guard that *key* belongs to this lambda, strip *strip_prefix*, prepend *dest_prefix*.

    *guard_prefix* (SOURCE_PREFIX) determines ownership — raises ValueError if the key
    does not match.  *strip_prefix* (STRIP_PREFIX) is the portion actually removed before
    *dest_prefix* is prepended; it may be deeper than *guard_prefix*.
    """
    if not key.startswith(guard_prefix):
        raise ValueError(
            f"Key '{key}' does not start with source prefix '{guard_prefix}'"
        )
    if not key.startswith(strip_prefix):
        raise ValueError(
            f"Key '{key}' does not start with strip prefix '{strip_prefix}'"
        )
    return dest_prefix + key[len(strip_prefix):]


def _handle_s3_record(
    s3_record: dict[str, Any],
    dest_bucket: str,
    guard_prefix: str,
    strip_prefix: str,
    dest_prefix: str,
) -> None:
    event_name: str = s3_record["eventName"]
    # Source bucket and key come from the event notification, not from env vars.
    source_bucket: str = s3_record["s3"]["bucket"]["name"]
    # Keys in S3 event notifications are URL-encoded.
    raw_key: str = s3_record["s3"]["object"]["key"]
    source_key = unquote_plus(raw_key)
    dest_key = transform_key(source_key, guard_prefix, strip_prefix, dest_prefix)
    s3 = _get_s3()

    if event_name.startswith("ObjectCreated"):
        logger.info(
            "Copying s3://%s/%s -> s3://%s/%s",
            source_bucket,
            source_key,
            dest_bucket,
            dest_key,
        )
        s3.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=dest_bucket,
            Key=dest_key,
        )
        logger.info("Copy complete: %s", dest_key)

    elif event_name.startswith("ObjectRemoved"):
        logger.info(
            "Deleting s3://%s/%s (triggered by removal of s3://%s/%s)",
            dest_bucket,
            dest_key,
            source_bucket,
            source_key,
        )
        try:
            s3.delete_object(Bucket=dest_bucket, Key=dest_key)
        except ClientError as exc:
            # 404 means object is already gone — treat as success.
            if exc.response["Error"]["Code"] == "NoSuchKey":
                logger.warning("Object already absent: s3://%s/%s", dest_bucket, dest_key)
            else:
                raise
        logger.info("Delete complete: %s", dest_key)

    else:
        logger.info("Ignoring event type '%s' for key '%s'", event_name, source_key)


def handler(event: dict[str, Any], context: object) -> dict[str, Any]:
    """SQS-triggered Lambda entry point.

    Returns a partial-batch-failure response so the SQS queue can retry
    individual failed messages without reprocessing successful ones.
    """
    dest_bucket = _require_env("DEST_BUCKET")
    guard_prefix = _require_env("SOURCE_PREFIX")
    # STRIP_PREFIX defaults to SOURCE_PREFIX when not set, preserving existing behaviour
    # for deployments that strip at the same level they guard on.
    strip_prefix = os.environ.get("STRIP_PREFIX") or guard_prefix
    dest_prefix = _require_env("DEST_PREFIX")

    batch_item_failures: list[dict[str, str]] = []

    for sqs_record in event.get("Records", []):
        message_id: str = sqs_record["messageId"]
        try:
            s3_event: dict[str, Any] = json.loads(sqs_record["body"])
            # S3 test notifications have no "Records" key — skip silently.
            for s3_record in s3_event.get("Records", []):
                _handle_s3_record(
                    s3_record,
                    dest_bucket,
                    guard_prefix,
                    strip_prefix,
                    dest_prefix,
                )
        except Exception:
            logger.exception("Failed to process SQS message %s", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
