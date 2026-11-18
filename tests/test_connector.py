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

    @staticmethod
    def truncate(table):
        return ("truncate", table, None)

    @staticmethod
    def delete(table=None, keys=None):
        return ("delete", table, keys)


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
    assert all(t["columns"]["user_id"] == "STRING" for t in tables[1:])


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
        if url.endswith("/v2/exports/contact-events"):
            # The old email events cursor is handed over once, never as `cursor`.
            assert cursor != "old-email-events-cursor"
            if cursor is None:
                et = body["types"][0]
                handoff = body.get("emailEventsCursor")
                assert handoff == ("old-email-events-cursor" if et == "EMAIL_SEND" else None)
                topic = et == "EMAIL_TOPIC_UNSUBSCRIBE"
                return _FakeResp(
                    _envelope(
                        {
                            "events": [
                                {
                                    "eventId": f"ev-{et}",
                                    "contactId": "c1",
                                    "occurredAt": "2026-06-01T10:00:00Z",
                                    "eventType": et,
                                    "parentType": "EMAIL",
                                    "sourceType": "BLAST",
                                    "sourceId": "blast1",
                                    "email": {
                                        "userId": "user-1",
                                        "emailId": "em1",
                                        "emailName": "Welcome",
                                        "sentEmailId": "se1",
                                        "isBot": False,
                                        "link": "http://x" if et == "EMAIL_CLICK" else None,
                                        "topicIds": ["t1", "t2"] if topic else None,
                                        "bounceType": None,
                                        "errorMessage": None,
                                    },
                                },
                            ]
                        },
                        f"ck-{et}-1",
                    )
                )
            return _FakeResp(_envelope({"events": []}, None))
        raise AssertionError(f"unexpected url {url}")

    patch_requests(handler)

    ops = list(connector.update(CONFIG, {"email_send_cursor": "old-email-events-cursor"}))
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
        assert rows[0]["user_id"] == "user-1"
        assert rows[0]["source_type"] == "BLAST" and "source" not in rows[0]
    click = [o[2] for o in upserts if o[1] == "email_click"][0]
    assert click["link"] == "http://x"
    topic = [o[2] for o in upserts if o[1] == "email_topic_unsubscribe"][0]
    assert topic["topic_ids"] == '["t1","t2"]'

    # Cursors advanced and were checkpointed.
    final = checkpoints[-1][1]
    assert final["contacts_cursor"] == "ck-2"
    for table, et in connector.EMAIL_STREAMS:
        assert final[f"{table}_contact_events_cursor"] == f"ck-{et}-1"
    assert "email_send_cursor" not in final


def test_contacts_apply_rows_in_order_and_a_new_walk_truncates(patch_requests):
    # A deleted row is deleted, not upserted, and rows apply in page order: c2 is
    # live and then deleted on the same page. A walk with no stored cursor
    # empties the table first.
    pages = {
        None: (
            {
                "contacts": [
                    {"id": "c1", "deleted": False, "fields": {}},
                    {"id": "c2", "deleted": False, "fields": {}},
                    {"id": "c2", "deleted": True, "deletedAt": "2026-11-01T00:00:00Z"},
                ],
            },
            "ck-1",
        ),
        "ck-1": (
            {
                "contacts": [
                    {"id": "c0", "deleted": True, "deletedAt": "2026-11-01T00:00:00Z"},
                    {"id": "c2", "deleted": False, "fields": {}},
                ],
            },
            None,
        ),
    }

    def handler(url, body):
        assert url.endswith("/v3/exports/contacts")
        data, next_cursor = pages[body.get("cursor")]
        return _FakeResp(_envelope(data, next_cursor))

    patch_requests(handler)
    only_contacts = {**CONFIG, "tables": "contacts", "version": "v3"}

    ops = [o for o in connector.update(only_contacts, {}) if o[0] != "checkpoint"]
    assert [(o[0], (o[2] or {}).get("id")) for o in ops] == [
        ("truncate", None),
        ("upsert", "c1"),
        ("upsert", "c2"),
        ("delete", "c2"),
        ("delete", "c0"),
        ("upsert", "c2"),
    ]
    assert "deleted" not in ops[1][2]

    # A stored cursor, including one from the v2 export, resumes without emptying the table.
    ops = list(connector.update(only_contacts, {"contacts_cursor": "ck-1"}))
    assert [o[0] for o in ops if o[0] != "checkpoint"] == ["delete", "upsert"]


def test_contacts_truncate_waits_for_the_first_page(patch_requests):
    # A failed first request leaves the table as it was.
    patch_requests(
        lambda url, body: _FakeResp({"error": {"code": "export_not_enabled"}}, status_code=403)
    )
    ops = []
    with pytest.raises(RuntimeError):
        for o in connector.update({**CONFIG, "tables": "contacts", "version": "v3"}, {}):
            ops.append(o)
    assert [o for o in ops if o[0] == "truncate"] == []


def test_contacts_expired_cursor_restarts_the_walk(patch_requests):
    # The stored cursor expired: the connector drops it, empties the table on the
    # restarted walk's first page, and stores the new walk's cursor.
    def handler(url, body):
        cursor = body.get("cursor")
        if cursor == "c1.old":
            return _FakeResp(
                {"error": {"code": "cursor_expired", "message": "expired"}}, status_code=400
            )
        if cursor is None:
            return _FakeResp(
                _envelope({"contacts": [{"id": "c1", "deleted": False, "fields": {}}]}, "c1.new")
            )
        return _FakeResp(_envelope({"contacts": []}, None))

    patch_requests(handler)
    v3 = {**CONFIG, "tables": "contacts", "version": "v3"}
    ops = list(connector.update(v3, {"contacts_v3_cursor": "c1.old"}))

    assert [(o[0], (o[2] or {}).get("id")) for o in ops if o[0] != "checkpoint"] == [
        ("truncate", None),
        ("upsert", "c1"),
    ]
    assert [o for o in ops if o[0] == "checkpoint"][-1][1]["contacts_v3_cursor"] == "c1.new"

    # An expired error without a stored cursor fails the sync instead of looping.
    patch_requests(
        lambda url, body: _FakeResp({"error": {"code": "cursor_expired"}}, status_code=400)
    )
    with pytest.raises(connector.CursorExpiredError):
        list(connector.update(v3, {}))

    # So does the restarted walk's own cursor expiring: the walk restarts once per sync.
    def always_expired(url, body):
        if body.get("cursor"):
            return _FakeResp({"error": {"code": "cursor_expired"}}, status_code=400)
        return _FakeResp(_envelope({"contacts": []}, "c1.fresh"))

    patch_requests(always_expired)
    with pytest.raises(connector.CursorExpiredError):
        list(connector.update(v3, {"contacts_v3_cursor": "c1.old"}))


def test_version_keeps_a_contacts_cursor_per_version(patch_requests):
    # v2 is the default and never deletes or truncates. v3 starts from the v2
    # cursor and keeps its own, so switching back to v2 resumes v2's cursor.
    pages = {
        ("/v2/exports/contacts", None): ([{"id": "c1", "fields": {}}], "v2-1"),
        ("/v2/exports/contacts", "v2-1"): ([], None),
        ("/v3/exports/contacts", "v2-1"): ([{"id": "c1", "deleted": True}], "c1.v3-1"),
        ("/v3/exports/contacts", "c1.v3-1"): ([], None),
    }
    calls = []

    def handler(url, body):
        key = (url.removeprefix(CONFIG["base_url"]), body.get("cursor"))
        calls.append(key)
        rows, next_cursor = pages[key]
        return _FakeResp(_envelope({"contacts": rows}, next_cursor))

    patch_requests(handler)
    v2 = {**CONFIG, "tables": "contacts"}
    v3 = {**v2, "version": "v3"}

    def sync(config, state):
        ops = list(connector.update(config, state))
        return [o[0] for o in ops if o[0] != "checkpoint"], [
            o for o in ops if o[0] == "checkpoint"
        ][-1][1]

    done, state = sync(v2, {})
    assert done == ["upsert"]
    assert state == {"contacts_cursor": "v2-1"}

    done, state = sync(v3, state)
    assert done == ["delete"], "a v2 cursor resumes on v3 without a truncate"
    assert state == {"contacts_cursor": "v2-1", "contacts_v3_cursor": "c1.v3-1"}

    calls.clear()
    done, state = sync(v2, state)
    assert calls[0] == ("/v2/exports/contacts", "v2-1")
    assert state["contacts_v3_cursor"] == "c1.v3-1", "switching back keeps the v3 cursor"

    with pytest.raises(ValueError):
        connector.schema({**CONFIG, "version": "v4"})


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


def test_contact_events_group_declares_one_table_per_family():
    cfg = {**CONFIG, "tables": "contact_events"}
    tables = {t["table"]: t for t in connector.schema(cfg)}
    assert list(tables) == [table for table, _, _, _ in connector.CONTACT_EVENT_STREAMS]
    assert all(t["primary_key"] == ["event_id"] for t in tables.values())
    assert tables["page_visits"]["columns"]["utm_extra"] == "JSON"
    assert "ip_address" not in tables["page_visits"]["columns"]
    assert tables["campaign_member_events"]["columns"]["is_success"] == "BOOLEAN"


def test_update_contact_events_filters_by_family_and_flattens(patch_requests):
    seen_bodies: list[dict] = []

    def handler(url, body):
        assert url.endswith("/v2/exports/contact-events"), url
        seen_bodies.append(body)
        if body.get("cursor") == "wf-1":
            return _FakeResp(_envelope({"events": []}, None))
        event = {
            "eventId": "e1",
            "contactId": "c1",
            "eventType": "WORKFLOW_ENTERED",
            "parentType": "WORKFLOW",
            "occurredAt": "2026-06-01T10:00:00.123456789Z",
            "createdAt": "2026-06-01T10:00:01Z",
            "sourceType": None,
            "sourceId": None,
            "workflow": {"workflowId": "w1", "workflowRunId": "r1", "workflowNodeId": "n1"},
        }
        return _FakeResp(_envelope({"events": [event]}, "wf-1"))

    patch_requests(handler)
    cfg = {**CONFIG, "tables": "workflow_events", "contact_events_start": "2025-01-01T00:00:00Z"}
    ops = list(connector.update(cfg, {}))

    assert seen_bodies[0] == {
        "limit": connector.PAGE_LIMIT,
        "parentTypes": ["WORKFLOW"],
        "occurredAt": {"start": "2025-01-01T00:00:00Z"},
    }
    assert seen_bodies[1] == {"limit": connector.PAGE_LIMIT, "cursor": "wf-1"}

    rows = [o for o in ops if o[0] == "upsert"]
    assert [o[1] for o in rows] == ["workflow_events"]
    row = rows[0][2]
    assert row["event_type"] == "WORKFLOW_ENTERED"
    assert row["workflow_id"] == "w1" and row["workflow_run_id"] == "r1"
    assert row["occurred_at"] == "2026-06-01T10:00:00.123456Z"
    assert [o for o in ops if o[0] == "checkpoint"][-1][1] == {"workflow_events_cursor": "wf-1"}


def test_assets_are_truncated_and_reread_every_sync(patch_requests):
    seen: list[tuple[str, dict]] = []
    pages = {
        "/v2/exports/campaign-types": {
            None: (
                {
                    "campaignTypes": [
                        {
                            "id": "t1",
                            "name": "Webinar",
                            "category": "WEBINAR",
                            "archived": False,
                            "statuses": [
                                {"id": "s1", "name": "Registered", "position": 0},
                                {"id": "s2", "name": "Attended", "position": 1, "isSuccess": True},
                            ],
                        }
                    ]
                },
                "ct-1",
            ),
            "ct-1": ({"campaignTypes": []}, None),
        },
        "/v2/exports/audience-members": {
            None: ({"members": [{"audienceId": "a1", "contactId": "c1"}]}, None),
        },
    }

    def handler(url, body):
        path = url.removeprefix(CONFIG["base_url"])
        seen.append((path, body))
        data, next_cursor = pages[path][body.get("cursor")]
        return _FakeResp(_envelope(data, next_cursor))

    patch_requests(handler)
    cfg = {**CONFIG, "tables": "campaign_types,campaign_type_statuses,audience_members"}
    ops = list(connector.update(cfg, {}))

    # Each table is truncated before its walk, and nothing else is called.
    assert [o[1] for o in ops if o[0] == "truncate"] == [
        "campaign_types",
        "campaign_type_statuses",
        "audience_members",
    ]
    assert [p for p, _ in seen] == [
        "/v2/exports/campaign-types",
        "/v2/exports/campaign-types",
        "/v2/exports/audience-members",
    ]
    upserts = [(o[1], o[2]) for o in ops if o[0] == "upsert"]
    assert upserts[0] == (
        "campaign_types",
        {
            "id": "t1",
            "name": "Webinar",
            "description": None,
            "category": "WEBINAR",
            "archived": False,
            "created_at": None,
            "updated_at": None,
        },
    )
    statuses = [row for table, row in upserts if table == "campaign_type_statuses"]
    assert [(r["id"], r["campaign_type_id"], r["position"]) for r in statuses] == [
        ("s1", "t1", 0),
        ("s2", "t1", 1),
    ]
    assert statuses[1]["is_success"] is True
    assert (
        "audience_members",
        {"audience_id": "a1", "contact_id": "c1", "created_at": None, "updated_at": None},
    ) in upserts

    # No asset cursor is kept, so the next sync starts from the beginning again.
    final = [o for o in ops if o[0] == "checkpoint"][-1][1]
    assert final == {}
    seen.clear()
    list(connector.update(cfg, final))
    assert all(
        "cursor" not in body for path, body in seen if path == "/v2/exports/audience-members"
    )
    assert seen[0] == ("/v2/exports/campaign-types", {"limit": connector.PAGE_LIMIT})


def test_asset_table_selection_skips_unlisted_tables(patch_requests):
    def handler(url, body):
        assert url.endswith("/v2/exports/campaign-types"), url
        return _FakeResp(
            _envelope({"campaignTypes": [{"id": "t1", "statuses": [{"id": "s1"}]}]}, None)
        )

    patch_requests(handler)
    ops = list(connector.update({**CONFIG, "tables": "campaign_type_statuses"}, {}))
    assert [o[1] for o in ops if o[0] == "truncate"] == ["campaign_type_statuses"]
    assert [(o[1], o[2]["id"], o[2]["campaign_type_id"]) for o in ops if o[0] == "upsert"] == [
        ("campaign_type_statuses", "s1", "t1")
    ]


def test_touchpoints_walk_from_the_configured_start(patch_requests):
    seen_bodies: list[dict] = []

    def handler(url, body):
        assert url.endswith("/v2/exports/touchpoints"), url
        seen_bodies.append(body)
        if body.get("cursor") == "tp-1":
            return _FakeResp(_envelope({"touchpoints": []}, "tp-1"))
        touchpoint = {
            "touchpointId": "ws:c1:s1",
            "contactId": "c1",
            "touchType": "WEB_SESSION",
            "occurredAt": "2026-06-01T10:00:00.123456789Z",
            "landingPageUrl": "https://example.com/pricing",
            "keyword": "crm software",
            "utmMedium": "cpc",
            "adProvider": "GOOGLE",
            "channelId": "ch1",
        }
        return _FakeResp(_envelope({"touchpoints": [touchpoint]}, "tp-1"))

    patch_requests(handler)
    cfg = {**CONFIG, "tables": "attribution", "touchpoints_start": "2025-01-01T00:00:00Z"}
    assert [t["table"] for t in connector.schema(cfg)] == ["touchpoints"]
    ops = list(connector.update(cfg, {}))

    assert seen_bodies[0] == {
        "limit": connector.PAGE_LIMIT,
        "occurredAt": {"start": "2025-01-01T00:00:00Z"},
    }
    # The server hands the same cursor back once caught up, which ends the walk.
    assert seen_bodies[1] == {"limit": connector.PAGE_LIMIT, "cursor": "tp-1"}
    assert len(seen_bodies) == 2

    row = [o[2] for o in ops if o[0] == "upsert"][0]
    assert row["touchpoint_id"] == "ws:c1:s1" and row["touch_type"] == "WEB_SESSION"
    assert row["landing_page_url"] == "https://example.com/pricing"
    assert row["keyword"] == "crm software" and row["utm_medium"] == "cpc"
    assert row["ad_provider"] == "GOOGLE" and row["channel_id"] == "ch1"
    assert row["occurred_at"] == "2026-06-01T10:00:00.123456Z"
    assert row["session_id"] is None
    assert [o for o in ops if o[0] == "checkpoint"][-1][1] == {"touchpoints_cursor": "tp-1"}


def test_email_handoff_keeps_old_cursor_until_the_new_walk_has_one(patch_requests):
    # Nothing new since the old cursor: no next cursor comes back, so the old
    # cursor stays and is handed over again on the next sync.
    sent = []

    def handler(url, body):
        assert url.endswith("/v2/exports/contact-events")
        sent.append(body)
        return _FakeResp(_envelope({"events": []}, None))

    patch_requests(handler)
    config = {**CONFIG, "tables": "email_send"}
    state = {"email_send_cursor": "old"}
    ops = list(connector.update(config, state))

    first = {"limit": connector.PAGE_LIMIT, "types": ["EMAIL_SEND"], "emailEventsCursor": "old"}
    assert sent == [first]
    final = [o for o in ops if o[0] == "checkpoint"][-1][1]
    assert final == {"email_send_cursor": "old"}
