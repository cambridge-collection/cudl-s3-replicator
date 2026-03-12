"""Tests for the S3 copy Lambda handler."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote_plus

import boto3
import pytest
from moto import mock_aws

import handler as h

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SOURCE_BUCKET = "source-bucket"
DEST_BUCKET = "dest-bucket"
SOURCE_PREFIX = "ui/"
DEST_PREFIX = "html/"


@pytest.fixture(autouse=True)
def env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEST_BUCKET", DEST_BUCKET)
    monkeypatch.setenv("SOURCE_PREFIX", SOURCE_PREFIX)
    monkeypatch.setenv("DEST_PREFIX", DEST_PREFIX)


@pytest.fixture(autouse=True)
def reset_s3_client() -> Any:
    """Force handler to create a fresh boto3 client inside each moto context."""
    h._s3 = None
    yield
    h._s3 = None


def _sqs_event(s3_records: list[dict[str, Any]], message_id: str = "msg-1") -> dict[str, Any]:
    """Wrap S3 event records in an SQS event envelope."""
    return {
        "Records": [
            {
                "messageId": message_id,
                "body": json.dumps({"Records": s3_records}),
            }
        ]
    }


def _s3_record(event_name: str, bucket: str, key: str) -> dict[str, Any]:
    return {
        "eventName": event_name,
        "s3": {
            "bucket": {"name": bucket},
            "object": {"key": quote_plus(key)},
        },
    }


# ---------------------------------------------------------------------------
# Unit tests: transform_key
# ---------------------------------------------------------------------------


def test_transform_key_strips_and_prepends() -> None:
    assert h.transform_key("ui/cudl-resources/a/b", "ui/", "ui/", "html/") == "html/cudl-resources/a/b"


def test_transform_key_strip_prefix_deeper_than_guard() -> None:
    assert h.transform_key("ui/cudl-resources/a/b", "ui/", "ui/cudl-resources/", "html/") == "html/a/b"


def test_transform_key_rejects_wrong_guard_prefix() -> None:
    with pytest.raises(ValueError, match="does not start with source prefix"):
        h.transform_key("other/path", "ui/", "ui/", "html/")


def test_transform_key_rejects_key_not_matching_strip_prefix() -> None:
    with pytest.raises(ValueError, match="does not start with strip prefix"):
        h.transform_key("ui/other/path", "ui/", "ui/cudl-resources/", "html/")


# ---------------------------------------------------------------------------
# Integration tests: ObjectCreated copies the object
# ---------------------------------------------------------------------------


@mock_aws
def test_object_created_copies_to_dest_bucket() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    source_key = "ui/cudl-resources/path/to/file/A"
    s3.put_object(Bucket=SOURCE_BUCKET, Key=source_key, Body=b"hello")

    event = _sqs_event([_s3_record("ObjectCreated:Put", SOURCE_BUCKET, source_key)])
    result = h.handler(event, object())

    assert result == {"batchItemFailures": []}

    dest_key = "html/cudl-resources/path/to/file/A"
    resp = s3.get_object(Bucket=DEST_BUCKET, Key=dest_key)
    assert resp["Body"].read() == b"hello"


# ---------------------------------------------------------------------------
# Integration tests: ObjectRemoved deletes from dest bucket
# ---------------------------------------------------------------------------


@mock_aws
def test_object_removed_deletes_from_dest_bucket() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    dest_key = "html/cudl-resources/path/to/file/A"
    s3.put_object(Bucket=DEST_BUCKET, Key=dest_key, Body=b"stale")

    source_key = "ui/cudl-resources/path/to/file/A"
    event = _sqs_event([_s3_record("ObjectRemoved:Delete", SOURCE_BUCKET, source_key)])
    result = h.handler(event, object())

    assert result == {"batchItemFailures": []}

    # Object should no longer exist in dest bucket.
    objects = s3.list_objects_v2(Bucket=DEST_BUCKET).get("Contents", [])
    assert not any(obj["Key"] == dest_key for obj in objects)


# ---------------------------------------------------------------------------
# Partial batch failure: bad message does not block good messages
# ---------------------------------------------------------------------------


@mock_aws
def test_partial_batch_failure_isolates_bad_message() -> None:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    good_key = "ui/cudl-resources/good.txt"
    s3.put_object(Bucket=SOURCE_BUCKET, Key=good_key, Body=b"ok")

    sqs_event: dict[str, Any] = {
        "Records": [
            # Bad message: body is not valid JSON.
            {"messageId": "bad-msg", "body": "not-json"},
            # Good message.
            {
                "messageId": "good-msg",
                "body": json.dumps(
                    {"Records": [_s3_record("ObjectCreated:Put", SOURCE_BUCKET, good_key)]}
                ),
            },
        ]
    }

    result = h.handler(sqs_event, object())

    assert result == {"batchItemFailures": [{"itemIdentifier": "bad-msg"}]}
    # Good message was still processed.
    s3.get_object(Bucket=DEST_BUCKET, Key="html/cudl-resources/good.txt")


# ---------------------------------------------------------------------------
# API contract test: handler always returns batchItemFailures key
# ---------------------------------------------------------------------------


@mock_aws
def test_handler_always_returns_batch_item_failures_key() -> None:
    """Contract: Lambda must return the batchItemFailures key for SQS partial-batch support."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    # Empty event — no records at all.
    result = h.handler({"Records": []}, object())
    assert "batchItemFailures" in result
    assert isinstance(result["batchItemFailures"], list)
