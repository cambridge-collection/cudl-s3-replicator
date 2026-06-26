"""Lambda handler: copies/deletes S3 objects in response to SQS-wrapped S3 events."""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


def _resolve_log_level() -> int:
    """LOG_LEVEL as a standard level name (e.g. DEBUG, INFO, WARNING); unset -> INFO, invalid -> error + INFO."""
    name = os.environ.get("LOG_LEVEL")
    if not name:
        return logging.INFO
    level = getattr(logging, name.upper(), None)
    if isinstance(level, int):
        return level
    logger.error("Invalid LOG_LEVEL %r; falling back to INFO", name)
    return logging.INFO


logger.setLevel(_resolve_log_level())

_s3: S3Client | None = None  # module-level client, replaced in tests


def _get_s3() -> S3Client:
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable '{name}' is not set")
    return value


def transform_key(key: str, strip_prefix: str, dest_prefix: str) -> str:
    """Strip *strip_prefix* off the front of *key* and prepend *dest_prefix*.

    Both prefixes default to empty, so with neither set the key passes through unchanged.
    Which objects reach the lambda is decided by the S3 event notification filter, not here.
    """
    return dest_prefix + key[len(strip_prefix) :]


def _handle_s3_record(
    s3_record: dict[str, Any],
    dest_bucket: str,
    strip_prefix: str,
    dest_prefix: str,
) -> None:
    event_name: str = s3_record["eventName"]
    # Source bucket and key come from the event notification, not from env vars.
    source_bucket: str = s3_record["s3"]["bucket"]["name"]
    # Keys in S3 event notifications are URL-encoded.
    raw_key: str = s3_record["s3"]["object"]["key"]
    source_key = unquote_plus(raw_key)
    dest_key = transform_key(source_key, strip_prefix, dest_prefix)
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
    strip_prefix = os.environ.get("STRIP_PREFIX") or os.environ.get("SOURCE_PREFIX") or ""
    dest_prefix = os.environ.get("DEST_PREFIX", "")
    if not os.environ.get("STRIP_PREFIX") and os.environ.get("SOURCE_PREFIX"):
        logger.warning("SOURCE_PREFIX is deprecated and will be removed; use STRIP_PREFIX.")

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
                    strip_prefix,
                    dest_prefix,
                )
        except Exception:
            logger.exception("Failed to process SQS message %s", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
