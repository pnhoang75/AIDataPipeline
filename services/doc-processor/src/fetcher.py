import base64
import logging
import time

logger = logging.getLogger(__name__)

_FETCH_RETRY_DELAYS = [1, 4, 16]


class FetchError(Exception):
    pass


def fetch_content(content_ref: str, s3_client=None) -> bytes:
    if content_ref.startswith("inline:"):
        return base64.b64decode(content_ref[len("inline:"):])
    elif content_ref.startswith("s3://"):
        return _fetch_s3(content_ref, s3_client)
    else:
        return _fetch_file(content_ref)


def fetch_content_with_retry(content_ref: str, s3_client=None) -> bytes:
    last_exc: Exception = FetchError("no attempts made")
    for delay in _FETCH_RETRY_DELAYS:
        try:
            return fetch_content(content_ref, s3_client)
        except FetchError as exc:
            last_exc = exc
            logger.warning("Content fetch failed, retrying in %ds: %s", delay, exc)
            time.sleep(delay)
    raise FetchError(f"Content fetch failed after retries: {last_exc}") from last_exc


def _fetch_s3(content_ref: str, s3_client) -> bytes:
    path = content_ref[len("s3://"):]
    parts = path.split("/", 1)
    if len(parts) != 2:
        raise FetchError(f"Invalid S3 ref: {content_ref}")
    bucket, key = parts
    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception as exc:
        raise FetchError(f"S3 fetch error: {exc}") from exc


def _fetch_file(path: str) -> bytes:
    try:
        with open(path, "rb") as f:
            return f.read()
    except OSError as exc:
        raise FetchError(f"File fetch error: {exc}") from exc
