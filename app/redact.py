"""Shared helper for logging stream identifiers without leaking the
embedded stream key.

`stream_id` / `path` / `provider_stream_id` across this service are all
the literal MediaMTX path, `live/<key>` (see main.py::create_stream) -
`<key>` IS the RTMP/WHIP stream key. Logging that value in full would put
a live credential in plaintext application logs, which the publisher
could use to hijack or disrupt someone else's broadcast. Every log
statement that includes a stream id must redact through this first.
"""

from __future__ import annotations


def redact_stream_id(stream_id: str) -> str:
    if not stream_id:
        return stream_id
    prefix, sep, key = stream_id.rpartition("/")
    if not sep:
        return "***" if len(stream_id) > 4 else stream_id
    if len(key) <= 4:
        return f"{prefix}/***"
    return f"{prefix}/...{key[-4:]}"
