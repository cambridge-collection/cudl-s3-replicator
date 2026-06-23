"""Tests for the S3 copy Lambda handler."""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib.parse import quote_plus

import boto3
import pytest
from moto import mock_aws
from pytest_mock import MockerFixture

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
    assert h.transform_key("ui/cudl-resources/a/b", "ui/", "html/") == "html/cudl-resources/a/b"


def test_transform_key_strips_deeper_prefix() -> None:
    assert h.transform_key("ui/cudl-resources/a/b", "ui/cudl-resources/", "html/") == "html/a/b"


def test_transform_key_identity_with_empty_prefixes() -> None:
    assert h.transform_key("any/path/file.txt", "", "") == "any/path/file.txt"


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


# ---------------------------------------------------------------------------
# Prefix resolution: identity copy, STRIP_PREFIX, SOURCE_PREFIX alias
# ---------------------------------------------------------------------------


@mock_aws
def test_identity_copy_with_only_dest_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOURCE_PREFIX", raising=False)
    monkeypatch.delenv("DEST_PREFIX", raising=False)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    key = "any/path/to/file.txt"
    s3.put_object(Bucket=SOURCE_BUCKET, Key=key, Body=b"verbatim")

    event = _sqs_event([_s3_record("ObjectCreated:Put", SOURCE_BUCKET, key)])
    assert h.handler(event, object()) == {"batchItemFailures": []}

    # Copied to the identical key in the dest bucket.
    assert s3.get_object(Bucket=DEST_BUCKET, Key=key)["Body"].read() == b"verbatim"


@mock_aws
def test_strip_prefix_is_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOURCE_PREFIX", raising=False)
    monkeypatch.setenv("STRIP_PREFIX", "data/")
    monkeypatch.setenv("DEST_PREFIX", "out/")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    s3.put_object(Bucket=SOURCE_BUCKET, Key="data/x/y", Body=b"z")

    event = _sqs_event([_s3_record("ObjectCreated:Put", SOURCE_BUCKET, "data/x/y")])
    assert h.handler(event, object()) == {"batchItemFailures": []}

    assert s3.get_object(Bucket=DEST_BUCKET, Key="out/x/y")["Body"].read() == b"z"


@mock_aws
def test_strip_prefix_overrides_source_prefix_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    # Both set: STRIP_PREFIX wins; SOURCE_PREFIX is ignored.
    monkeypatch.setenv("SOURCE_PREFIX", "wrong-and-longer/")
    monkeypatch.setenv("STRIP_PREFIX", "a/")
    monkeypatch.setenv("DEST_PREFIX", "html/")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    s3.put_object(Bucket=SOURCE_BUCKET, Key="a/foo", Body=b"v")

    event = _sqs_event([_s3_record("ObjectCreated:Put", SOURCE_BUCKET, "a/foo")])
    assert h.handler(event, object()) == {"batchItemFailures": []}

    # Stripped by STRIP_PREFIX ("a/"), not SOURCE_PREFIX.
    assert s3.get_object(Bucket=DEST_BUCKET, Key="html/foo")["Body"].read() == b"v"


def test_source_prefix_alias_logs_deprecation_warning(caplog: pytest.LogCaptureFixture) -> None:
    # autouse env_vars sets SOURCE_PREFIX with no STRIP_PREFIX.
    with caplog.at_level(logging.WARNING, logger="handler"):
        h.handler({"Records": []}, object())
    assert "SOURCE_PREFIX is deprecated" in caplog.text


def test_strip_prefix_set_suppresses_deprecation_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("STRIP_PREFIX", "ui/")
    with caplog.at_level(logging.WARNING, logger="handler"):
        h.handler({"Records": []}, object())
    assert "SOURCE_PREFIX is deprecated" not in caplog.text


def test_source_prefix_alias_assigns_internal_strip_prefix(
    monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture
) -> None:
    # With SOURCE_PREFIX set and STRIP_PREFIX unset, the internal strip prefix passed
    # downstream must be the SOURCE_PREFIX value.
    monkeypatch.delenv("STRIP_PREFIX", raising=False)
    monkeypatch.setenv("SOURCE_PREFIX", "tei-assets/")
    monkeypatch.setenv("DEST_PREFIX", "html/")
    spy = mocker.patch.object(h, "_handle_s3_record")

    event = _sqs_event([_s3_record("ObjectCreated:Put", SOURCE_BUCKET, "tei-assets/x")])
    h.handler(event, object())

    # _handle_s3_record(s3_record, dest_bucket, strip_prefix, dest_prefix)
    _record, _dest_bucket, strip_prefix, dest_prefix = spy.call_args.args
    assert strip_prefix == "tei-assets/"
    assert dest_prefix == "html/"


@mock_aws
def test_live_tei_assets_config_backcompat(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pins the live cudl-copy-tei-assets deployment: SOURCE_PREFIX alias, no STRIP_PREFIX.
    monkeypatch.delenv("STRIP_PREFIX", raising=False)
    monkeypatch.setenv("SOURCE_PREFIX", "tei-assets/")
    monkeypatch.setenv("DEST_PREFIX", "html/cudl-resources/")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=SOURCE_BUCKET)
    s3.create_bucket(Bucket=DEST_BUCKET)

    source_key = "tei-assets/MS-ADD/MS-ADD-00001.xml"
    s3.put_object(Bucket=SOURCE_BUCKET, Key=source_key, Body=b"<tei/>")

    event = _sqs_event([_s3_record("ObjectCreated:Put", SOURCE_BUCKET, source_key)])
    assert h.handler(event, object()) == {"batchItemFailures": []}

    dest_key = "html/cudl-resources/MS-ADD/MS-ADD-00001.xml"
    assert s3.get_object(Bucket=DEST_BUCKET, Key=dest_key)["Body"].read() == b"<tei/>"


def test_missing_dest_bucket_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEST_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="DEST_BUCKET"):
        h.handler({"Records": []}, object())
