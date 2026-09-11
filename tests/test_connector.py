"""Unit tests for the Conversion Fivetran connector.

The Fivetran SDK and the HTTP client are stubbed so the connector's schema,
pagination, cursor checkpointing, and row mapping can be exercised without the
SDK runtime or a live API.
"""

import sys
import types

import pytest

# --- Stub the Fivetran SDK before importing the connector ------------------ #
_sdk = types.ModuleType("fivetran_connector_sdk")


class _Connector:
    def __init__(self, update=None, schema=None):
        self.update, self.schema = update, schema


class _Logging:
    @staticmethod
    def info(*a, **k): ...

    @staticmethod
    def warning(*a, **k): ...

    @staticmethod
    def error(*a, **k): ...


class _Operations:
    @staticmethod
    def upsert(table=None, data=None):
        return ("upsert", table, data)

    @staticmethod
    def checkpoint(state):
        return ("checkpoint", dict(state))


_sdk.Connector = _Connector
_sdk.Logging = _Logging
_sdk.Operations = _Operations
sys.modules["fivetran_connector_sdk"] = _sdk

import connector  # noqa: E402  (must import after stubbing the SDK)


# --- Fake requests --------------------------------------------------------- #
class _RequestException(Exception): ...


class _HTTPError(_RequestException): ...


class _FakeResp:
    def __init__(self, payload, status_code=200):
        self._payload, self.status_code, self.text = payload, status_code, ""

    def json(self):
        return self._payload


def _envelope(data, next_cursor=None):
    return {"data": data, "pagination": {"nextCursor": next_cursor}, "error": None}


class _FakeRequests:
    """Minimal stand-in for the `requests` module used by connector.py."""

    RequestException = _RequestException
    HTTPError = _HTTPError

    def __init__(self, handler):
        self._handler = handler

    def post(self, url, json=None, headers=None, timeout=None):
        return self._handler(url, json or {})


@pytest.fixture
def patch_requests(monkeypatch):
    """Install a fake requests module with a programmable POST handler."""

    def _install(handler):
        monkeypatch.setattr(connector, "requests", _FakeRequests(handler))

    return _install


CONFIG = {"base_url": "https://pub-api.conversion.ai/api", "api_key": "sk_live_test"}


def test_schema_declares_contacts_and_email_tables():
    tables = connector.schema(CONFIG)
    names = [t["table"] for t in tables]
    # contacts plus one table per email event stream, in EMAIL_STREAMS order.
    assert names == ["contacts"] + [table for table, _ in connector.EMAIL_STREAMS]
    assert tables[0]["primary_key"] == ["id"]
    assert all(t["primary_key"] == ["event_id"] for t in tables[1:])


def test_schema_requires_config():
    with pytest.raises(ValueError):
        connector.schema({"base_url": "x"})  # missing api_key


def test_tables_config_selects_tables():
    # Unset -> the pre-existing set (contacts + email), never custom_events.
    assert connector._resolve_tables(CONFIG) == {"contacts", *dict(connector.EMAIL_STREAMS)}
    # A customer that only wants custom events gets exactly that table.
    only_custom = {**CONFIG, "tables": "custom_events"}
    assert connector._resolve_tables(only_custom) == {"custom_events"}
    assert [t["table"] for t in connector.schema(only_custom)] == ["custom_events"]
    # Groups and individual names mix; whitespace is tolerated.
    mixed = {**CONFIG, "tables": " contacts, email_click ,custom_events "}
    assert connector._resolve_tables(mixed) == {"contacts", "email_click", "custom_events"}
    # Unknown names and empty selections fail fast.
    with pytest.raises(ValueError, match="unknown table 'bogus'"):
        connector._resolve_tables({**CONFIG, "tables": "contacts,bogus"})
    with pytest.raises(ValueError, match="selects no tables"):
        connector._resolve_tables({**CONFIG, "tables": " , "})


def test_update_custom_events_only_sends_range_once_and_skips_other_tables(patch_requests):
    # Page 1 must carry occurredAt.start and no cursor; later pages carry only
    # the cursor (the server rejects both together). A resumed sync (cursor in
    # state) must also omit occurredAt.
    seen_bodies: list[dict] = []
    pages = {
        None: (
            {
                "events": [
                    {
                        "eventId": "ce1",
                        "contactId": "c1",
                        "occurredAt": "2026-06-01T10:00:00.123456789Z",
                        "createdAt": "2026-06-01T10:00:01Z",
                        "eventName": "signed_up",
                        "source": "API",
                        "contactEmail": "a@x.com",
                        "userId": "u-1",
                        "clientEventId": "msg-1",
                        "data": {"plan": "pro", "seats": 3},
                    }
                ]
            },
            "cur-1",
        ),
        "cur-1": ({"events": [{"eventId": "ce2", "contactId": "c2", "data": None}]}, None),
    }

    def handler(url, body):
        assert url.endswith("/v2/exports/custom-events"), url  # no other tables hit
        seen_bodies.append(body)
        data, next_cursor = pages[body.get("cursor")]
        return _FakeResp(_envelope(data, next_cursor))

    patch_requests(handler)
    ops = list(connector.update({**CONFIG, "tables": "custom_events"}, {}))

    assert seen_bodies[0] == {
        "limit": connector.PAGE_LIMIT,
        "occurredAt": {"start": connector.CUSTOM_EVENTS_START},
    }
    assert seen_bodies[1] == {"limit": connector.PAGE_LIMIT, "cursor": "cur-1"}

    rows = [o[2] for o in ops if o[0] == "upsert"]
    assert [r["event_id"] for r in rows] == ["ce1", "ce2"]
    assert all(o[1] == "custom_events" for o in ops if o[0] == "upsert")
    assert rows[0]["event_name"] == "signed_up" and rows[0]["source"] == "API"
    assert rows[0]["contact_email"] == "a@x.com" and rows[0]["user_id"] == "u-1"
    assert rows[0]["client_event_id"] == "msg-1"
    assert rows[0]["occurred_at"] == "2026-06-01T10:00:00.123456Z"
    # `data` is passed through as the parsed object: the SDK json.dumps declared
    # JSON columns itself, so a pre-serialized string would be double-encoded.
    assert rows[0]["data"] == {"plan": "pro", "seats": 3}
    assert rows[1]["data"] is None

    final = [o for o in ops if o[0] == "checkpoint"][-1][1]
    assert final == {"custom_events_cursor": "cur-1"}

    # Resume: the stored cursor goes out alone.
    seen_bodies.clear()
    list(connector.update({**CONFIG, "tables": "custom_events"}, final))
    assert seen_bodies == [{"limit": connector.PAGE_LIMIT, "cursor": "cur-1"}]


def test_custom_events_start_config(patch_requests):
    # A configured start replaces the default on the first request only.
    seen_bodies: list[dict] = []

    def handler(url, body):
        seen_bodies.append(body)
        return _FakeResp(_envelope({"events": []}, None))

    patch_requests(handler)
    cfg = {**CONFIG, "tables": "custom_events", "custom_events_start": "2025-06-01T00:00:00Z"}
    list(connector.update(cfg, {}))
    assert seen_bodies == [
        {"limit": connector.PAGE_LIMIT, "occurredAt": {"start": "2025-06-01T00:00:00Z"}}
    ]

    # Unset -> default; malformed -> fails at schema time, before any sync.
    assert connector._resolve_custom_events_start(CONFIG) == connector.CUSTOM_EVENTS_START
    with pytest.raises(ValueError, match="custom_events_start"):
        connector.schema({**CONFIG, "custom_events_start": "2025-06-01"})


def test_update_flattens_fields_splits_sfdc_and_paginates(patch_requests):
    # Contacts arrive across three pages. Page 2 is SHORT (one row, as if a
    # StarRocks id was missing from Spanner) yet still returns a `nextCursor`,
    # and page 3 has more data — so a connector that stopped on a short page
    # would drop c4. The connector pages until the cursor is exhausted.
    #
    # Each entry maps the request `cursor` to (response data, nextCursor).
    contact_pages = {
        None: (
            {
                "contacts": [
                    {
                        "id": "c1",
                        "email": "a@x.com",
                        "sfdcLeadId": "00Q1",
                        "sfdcContactId": None,
                        "sfdcAccountId": "001ACME",
                        "subscriptionStatus": "SUBSCRIBED",
                        "createdAt": "2026-01-01T00:00:00Z",
                        "updatedAt": "2026-06-01T00:00:00Z",
                        "fields": {"owner_id": "u1", "first_name": "Ada"},
                    },
                    {
                        "id": "c2",
                        "email": "b@x.com",
                        "sfdcLeadId": None,
                        "sfdcContactId": "0031",
                        "sfdcAccountId": None,
                        "subscriptionStatus": "NO_STATUS",
                        "createdAt": "2026-01-02T00:00:00Z",
                        "updatedAt": "2026-06-02T00:00:00Z",
                        "fields": {"owner_id": "u2"},
                    },
                ]
            },
            "ck-1",
        ),
        # short page (Spanner miss), but the cursor still advances
        "ck-1": ({"contacts": [{"id": "c3", "email": "c@x.com", "fields": {}}]}, "ck-2"),
        # cursor exhausted
        "ck-2": ({"contacts": [{"id": "c4", "email": "d@x.com", "fields": {}}]}, None),
    }

    def handler(url, body):
        cursor = body.get("cursor")
        if url.endswith("/v2/exports/contacts"):
            data, next_cursor = contact_pages[cursor]
            return _FakeResp(_envelope(data, next_cursor))
        if url.endswith("/v2/exports/email-events"):
            et = body["eventType"]
            if cursor is None:
                return _FakeResp(
                    _envelope(
                        {
                            "events": [
                                {
                                    "eventId": f"ev-{et}",
                                    "contactId": "c1",
                                    "occurredAt": "2026-06-01T10:00:00Z",
                                    "eventType": et,
                                    "sourceType": "BLAST",
                                    "sourceId": "blast1",
                                    "emailId": "em1",
                                    "emailName": "Welcome",
                                    "sentEmailId": "se1",
                                    "isBot": False,
                                    "link": "http://x" if et == "EMAIL_CLICK" else None,
                                    "topicIds": None,
                                    "bounceType": None,
                                    "errorMessage": None,
                                },
                            ]
                        },
                        f"ck-{et}-1",
                    )
                )
            return _FakeResp(_envelope({"events": []}, None))
        raise AssertionError(f"unexpected url {url}")

    patch_requests(handler)

    ops = list(connector.update(CONFIG, {}))
    upserts = [o for o in ops if o[0] == "upsert"]
    checkpoints = [o for o in ops if o[0] == "checkpoint"]

    # All four contacts arrive — the short middle page did not end the stream.
    contacts = [o[2] for o in upserts if o[1] == "contacts"]
    assert [c["id"] for c in contacts] == ["c1", "c2", "c3", "c4"]
    assert contacts[0]["owner_id"] == "u1" and contacts[0]["first_name"] == "Ada"
    assert contacts[0]["sfdc_lead_id"] == "00Q1" and contacts[0]["sfdc_contact_id"] is None
    assert contacts[1]["sfdc_contact_id"] == "0031" and contacts[1]["sfdc_lead_id"] is None
    assert contacts[0]["sfdc_account_id"] == "001ACME" and contacts[1]["sfdc_account_id"] is None
    assert "company_id" not in contacts[0]  # conversion company id intentionally dropped

    # Each email stream produced one mapped row, with the resolved asset name.
    for table, et in connector.EMAIL_STREAMS:
        rows = [o[2] for o in upserts if o[1] == table]
        assert len(rows) == 1 and rows[0]["event_id"] == f"ev-{et}"
        assert rows[0]["event_type"] == et
        assert rows[0]["email_name"] == "Welcome"
        assert rows[0]["source_type"] == "BLAST" and "source" not in rows[0]
    click = [o[2] for o in upserts if o[1] == "email_click"][0]
    assert click["link"] == "http://x"

    # Cursors advanced and were checkpointed.
    final = checkpoints[-1][1]
    assert final["contacts_cursor"] == "ck-2"
    for table, et in connector.EMAIL_STREAMS:
        assert final[f"{table}_cursor"] == f"ck-{et}-1"


def test_timestamps_truncated_to_microseconds():
    # Fivetran's UTC_DATETIME parser only accepts <=6 fractional digits, but the
    # API emits nanoseconds. Mappers must truncate the fraction (keeping the
    # timezone) so op.upsert can parse the value.
    contact = connector._map_contact(
        {
            "id": "c1",
            "createdAt": "2026-06-18T16:53:07.353963682Z",
            "updatedAt": "2026-06-18T16:53:07.123456+00:00",  # already 6 digits
            "fields": {},
        }
    )
    assert contact["created_at"] == "2026-06-18T16:53:07.353963Z"
    assert contact["updated_at"] == "2026-06-18T16:53:07.123456+00:00"

    event = connector._map_email_event(
        {"eventId": "e1", "occurredAt": "2026-06-18T16:53:07.999999999Z"}
    )
    assert event["occurred_at"] == "2026-06-18T16:53:07.999999Z"

    # No sub-second fraction and non-string values pass through untouched.
    assert connector._map_email_event({"occurredAt": "2026-06-18T16:53:07Z"})["occurred_at"] == (
        "2026-06-18T16:53:07Z"
    )
    assert connector._map_email_event({"occurredAt": None})["occurred_at"] is None


def test_post_retries_transient_failure(patch_requests, monkeypatch):
    monkeypatch.setattr(connector.time, "sleep", lambda *_: None)  # no real backoff
    calls = {"n": 0}

    def handler(url, body):
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeResp({}, status_code=500)  # transient -> retried
        return _FakeResp(_envelope({"contacts": []}, None))

    patch_requests(handler)
    payload = connector._post(CONFIG["base_url"], CONFIG["api_key"], "/v2/exports/contacts", {})
    assert calls["n"] == 2
    # `_post` returns the full envelope; callers read rows from `data` and the
    # next cursor from `pagination.nextCursor`.
    assert payload == _envelope({"contacts": []}, None)
