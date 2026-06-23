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


def test_schema_declares_six_tables():
    tables = connector.schema(CONFIG)
    names = [t["table"] for t in tables]
    assert names == [
        "contacts",
        "email_send",
        "email_open",
        "email_click",
        "email_delivered",
        "email_unsubscribe",
    ]
    assert tables[0]["primary_key"] == ["id"]
    assert all(t["primary_key"] == ["event_id"] for t in tables[1:])


def test_schema_requires_config():
    with pytest.raises(ValueError):
        connector.schema({"base_url": "x"})  # missing api_key


def test_update_flattens_variables_splits_sfdc_and_paginates(patch_requests):
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
                        "variables": {"owner_id": "u1", "ajs_anonymous_id": "anon1"},
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
                        "variables": {"owner_id": "u2"},
                    },
                ]
            },
            "ck-1",
        ),
        # short page (Spanner miss), but the cursor still advances
        "ck-1": ({"contacts": [{"id": "c3", "email": "c@x.com", "variables": {}}]}, "ck-2"),
        # cursor exhausted
        "ck-2": ({"contacts": [{"id": "c4", "email": "d@x.com", "variables": {}}]}, None),
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
                                    "eventType": "EMAIL_" + et,
                                    "sourceType": "BLAST",
                                    "sourceId": "blast1",
                                    "emailId": "em1",
                                    "emailName": "Welcome",
                                    "sentEmailId": "se1",
                                    "isBot": False,
                                    "link": "http://x" if et == "CLICK" else None,
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
    assert contacts[0]["owner_id"] == "u1" and contacts[0]["ajs_anonymous_id"] == "anon1"
    assert contacts[0]["sfdc_lead_id"] == "00Q1" and contacts[0]["sfdc_contact_id"] is None
    assert contacts[1]["sfdc_contact_id"] == "0031" and contacts[1]["sfdc_lead_id"] is None
    assert contacts[0]["sfdc_account_id"] == "001ACME" and contacts[1]["sfdc_account_id"] is None
    assert "company_id" not in contacts[0]  # conversion company id intentionally dropped

    # Each email stream produced one mapped row, with the resolved asset name.
    for table, et in [
        ("email_send", "SEND"),
        ("email_open", "OPEN"),
        ("email_click", "CLICK"),
        ("email_delivered", "DELIVERED"),
        ("email_unsubscribe", "UNSUBSCRIBE"),
    ]:
        rows = [o[2] for o in upserts if o[1] == table]
        assert len(rows) == 1 and rows[0]["event_id"] == f"ev-{et}"
        assert rows[0]["email_name"] == "Welcome"
        assert rows[0]["source_type"] == "BLAST" and "source" not in rows[0]
    click = [o[2] for o in upserts if o[1] == "email_click"][0]
    assert click["link"] == "http://x"

    # Cursors advanced and were checkpointed.
    final = checkpoints[-1][1]
    assert final["contacts_cursor"] == "ck-2"
    for table, et in [
        ("email_send", "SEND"),
        ("email_open", "OPEN"),
        ("email_click", "CLICK"),
        ("email_delivered", "DELIVERED"),
        ("email_unsubscribe", "UNSUBSCRIBE"),
    ]:
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
            "variables": {},
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
