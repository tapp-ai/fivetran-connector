r"""Fivetran Connector SDK connector for Conversion.

Exports up to eleven tables from the Conversion public API into a destination
warehouse:

  - contacts   one row per contact, with every contact field
               flattened in as a column keyed by its common name (e.g. owner_id,
               first_name, ...).
  - nine per-event email tables (email_send, email_delivery, email_open,
    email_click, email_bounce, email_soft_bounce, email_complaint,
    email_unsubscribe_all, email_topic_unsubscribe), each one row per email
    engagement event of a single EMAIL_* type.
    See EMAIL_STREAMS for the table -> eventType mapping.
  - custom_events  one row per custom (behavioral) event recorded against a
               contact. Opt-in: see "Table selection" below.

All data is read from the public API using an API key (X-API-Key). The API
scopes every request to the business that owns the key, so no business id is
configured here.

Incremental sync
----------------
Each table keeps its own opaque cursor in connector ``state``, keyed by table
(``contacts_cursor``, ``email_send_cursor``, ...). The connector never
interprets the cursor — it stores whatever the API last returned and sends it
back as the ``cursor`` on the next request (alongside ``limit`` and, for the
email tables, ``eventType``).

The API returns ``pagination.nextCursor`` whenever there are more results; we
persist it and checkpoint after every page, then resume from the stored cursor
on the next sync. Paging stops only once the cursor is exhausted (null) or stops
advancing — a short page is NOT end-of-stream. Rows are upserted by primary key
(``id`` / ``event_id``), so any row the API re-emits updates in place.

The custom_events endpoint is a windowed walk rather than a plain cursor: the
FIRST request carries ``occurredAt.start`` and every later request carries only
the cursor. We send a fixed early start with no end, so the server re-resolves
``now()`` on every request and the stored cursor keeps advancing into new events
across syncs exactly like the other tables.

Table selection
---------------
The optional ``tables`` configuration value is a comma-separated list of table
names and/or groups (``contacts``, ``email``, ``custom_events``; ``email``
expands to every ``email_*`` table). Unlisted tables are neither declared in the
schema nor synced. When ``tables`` is unset the connector syncs ``contacts`` and
``email`` — exactly what it synced before this option existed — so existing
connections are unaffected by a redeploy.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Iterable
from typing import Any

import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Page size requested from the API. The server applies its own hard cap.
PAGE_LIMIT = 1000

# HTTP retry policy for transient failures (network errors / 5xx / 429).
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2
REQUEST_TIMEOUT_SECONDS = 60

# The per-event email tables and the EMAIL_* eventType each one requests. The
# destination table name is the lowercased event type. eventType is matched 1:1
# against the underlying email event type; see the Conversion export API docs
# for the full set of exportable types.
EMAIL_STREAMS = [
    ("email_send", "EMAIL_SEND"),
    ("email_delivery", "EMAIL_DELIVERY"),
    ("email_open", "EMAIL_OPEN"),
    ("email_click", "EMAIL_CLICK"),
    ("email_bounce", "EMAIL_BOUNCE"),
    ("email_soft_bounce", "EMAIL_SOFT_BOUNCE"),
    ("email_complaint", "EMAIL_COMPLAINT"),
    ("email_unsubscribe_all", "EMAIL_UNSUBSCRIBE_ALL"),
    ("email_topic_unsubscribe", "EMAIL_TOPIC_UNSUBSCRIBE"),
]

# Fixed floor for the custom_events walk. Sent only on the very first request
# (before any cursor exists); afterwards the cursor carries the range. The
# server bounds each internal query, so a long range is safe.
CUSTOM_EVENTS_START = "2020-01-01T00:00:00Z"

# Table groups accepted in the `tables` configuration value. Individual table
# names are also accepted.
TABLE_GROUPS: dict[str, list[str]] = {
    "contacts": ["contacts"],
    "email": [table for table, _ in EMAIL_STREAMS],
    "custom_events": ["custom_events"],
}
ALL_TABLES: list[str] = [t for group in TABLE_GROUPS.values() for t in group]

# What syncs when `tables` is unset: the pre-existing table set.
DEFAULT_TABLES = "contacts,email"


def _resolve_tables(configuration: dict) -> set[str]:
    """Return the set of destination tables enabled by ``configuration``.

    Fivetran passes every configuration value as a string, so ``tables`` is a
    comma-separated list. Unknown names fail fast rather than silently syncing
    nothing.
    """
    raw = (configuration.get("tables") or DEFAULT_TABLES).strip()
    enabled: set[str] = set()
    for name in (part.strip() for part in raw.split(",")):
        if not name:
            continue
        if name in TABLE_GROUPS:
            enabled.update(TABLE_GROUPS[name])
        elif name in ALL_TABLES:
            enabled.add(name)
        else:
            raise ValueError(
                f"unknown table '{name}' in configuration 'tables'; "
                f"expected any of {', '.join(list(TABLE_GROUPS) + ALL_TABLES)}"
            )
    if not enabled:
        raise ValueError("configuration 'tables' selects no tables")
    return enabled


# --------------------------------------------------------------------------- #
#                                   Schema                                     #
# --------------------------------------------------------------------------- #
def schema(configuration: dict) -> list[dict]:
    """Declare the destination tables and their primary keys.

    Only the stable, known columns are typed here. Contact field columns are
    intentionally left undeclared so Fivetran infers them from the upserted
    data — that is what lets every field surface as its own column keyed by its
    common name without hardcoding the set.
    """
    _require_config(configuration)
    enabled = _resolve_tables(configuration)

    tables: list[dict] = (
        [
            {
                "table": "contacts",
                "primary_key": ["id"],
                "columns": {
                    "id": "STRING",
                    "sfdc_lead_id": "STRING",
                    "sfdc_contact_id": "STRING",
                    "sfdc_account_id": "STRING",
                    "email": "STRING",
                    "subscription_status": "STRING",
                    "created_at": "UTC_DATETIME",
                    "updated_at": "UTC_DATETIME",
                },
            },
        ]
        + [
            {
                "table": table,
                "primary_key": ["event_id"],
                "columns": {
                    "event_id": "STRING",
                    "contact_id": "STRING",
                    "occurred_at": "UTC_DATETIME",
                    "event_type": "STRING",
                    "source_type": "STRING",
                    "source_id": "STRING",
                    "email_id": "STRING",
                    "email_name": "STRING",
                    "sent_email_id": "STRING",
                    "is_bot": "BOOLEAN",
                    "link": "STRING",
                    "topic_ids": "STRING",
                    "bounce_type": "STRING",
                    "error_message": "STRING",
                },
            }
            for table, _event_type in EMAIL_STREAMS
        ]
        + [
            {
                "table": "custom_events",
                "primary_key": ["event_id"],
                "columns": {
                    "event_id": "STRING",
                    "contact_id": "STRING",
                    "occurred_at": "UTC_DATETIME",
                    "created_at": "UTC_DATETIME",
                    "event_name": "STRING",
                    "source": "STRING",
                    "contact_email": "STRING",
                    "user_id": "STRING",
                    "client_event_id": "STRING",
                    # The event payload object; its shape varies per event.
                    "data": "JSON",
                },
            },
        ]
    )
    return [t for t in tables if t["table"] in enabled]


# --------------------------------------------------------------------------- #
#                                   Update                                     #
# --------------------------------------------------------------------------- #
def update(configuration: dict, state: dict) -> Iterable[Any]:
    """Sync all tables, newest progress checkpointed after every page."""
    _require_config(configuration)
    base_url = configuration["base_url"].rstrip("/")
    api_key = configuration["api_key"]
    state = dict(state or {})
    enabled = _resolve_tables(configuration)

    log.info(f"Conversion connector: starting sync of {', '.join(sorted(enabled))}")

    # Contacts ---------------------------------------------------------------
    if "contacts" in enabled:
        yield from _sync_stream(
            base_url=base_url,
            api_key=api_key,
            table="contacts",
            path="/v2/exports/contacts",
            body_extra={},
            rows_key="contacts",
            state=state,
            state_key="contacts_cursor",
            row_mapper=_map_contact,
        )

    # Email events -----------------------------------------------------------
    for table, event_type in EMAIL_STREAMS:
        if table not in enabled:
            continue
        yield from _sync_stream(
            base_url=base_url,
            api_key=api_key,
            table=table,
            path="/v2/exports/email-events",
            body_extra={"eventType": event_type},
            rows_key="events",
            state=state,
            state_key=f"{table}_cursor",
            row_mapper=_map_email_event,
        )

    # Custom events ----------------------------------------------------------
    # Windowed endpoint: occurredAt goes on the first request only; the cursor
    # carries the range afterwards and the two are mutually exclusive.
    if "custom_events" in enabled:
        yield from _sync_stream(
            base_url=base_url,
            api_key=api_key,
            table="custom_events",
            path="/v2/exports/custom-events",
            body_extra={},
            rows_key="events",
            state=state,
            state_key="custom_events_cursor",
            row_mapper=_map_custom_event,
            first_page_extra={"occurredAt": {"start": CUSTOM_EVENTS_START}},
        )

    log.info("Conversion connector: sync complete")


def _sync_stream(
    base_url: str,
    api_key: str,
    table: str,
    path: str,
    body_extra: dict[str, Any],
    rows_key: str,
    state: dict,
    state_key: str,
    row_mapper: Callable[[dict], dict],
    first_page_extra: dict[str, Any] | None = None,
) -> Iterable[Any]:
    """Page one table to exhaustion, upserting rows and checkpointing cursors.

    ``body_extra`` is sent on every request; ``first_page_extra`` only when no
    cursor exists yet (for endpoints whose cursor carries the initial range).

    Paging is driven by the API's ``pagination.nextCursor``, NOT by page length —
    a short page is not end-of-stream. ListContactsV5 silently drops ids that are
    in StarRocks but missing from Spanner, so a full upstream page can come back
    with fewer rows while ``nextCursor`` still points to more. We stop only when
    the cursor is exhausted (null) or stops advancing. Every page is checkpointed,
    so rows are flushed and progress resumes on the next sync.
    """
    cursor: str | None = state.get(state_key)
    page_count = 0
    row_count = 0

    while True:
        body: dict[str, Any] = {"limit": PAGE_LIMIT, **body_extra}
        if cursor:
            body["cursor"] = cursor
        elif first_page_extra:
            body.update(first_page_extra)

        payload = _post(base_url, api_key, path, body)
        data = payload.get("data") or {}
        rows = data.get(rows_key) or []

        for raw in rows:
            yield op.upsert(table=table, data=row_mapper(raw))

        row_count += len(rows)
        page_count += 1

        next_cursor = (payload.get("pagination") or {}).get("nextCursor")
        advanced = bool(next_cursor) and next_cursor != cursor
        if advanced:
            cursor = next_cursor
            state[state_key] = cursor
        yield op.checkpoint(state)

        # Stop only once the cursor is exhausted (or stalls); a short page still
        # continues as long as `next` advances.
        if not advanced:
            break

    log.info(f"{table}: synced {row_count} rows across {page_count} page(s)")


# --------------------------------------------------------------------------- #
#                                Row mappers                                   #
# --------------------------------------------------------------------------- #
# The API emits RFC-3339 timestamps with nanosecond precision (e.g.
# "2026-06-18T16:53:07.353963682Z"), but Fivetran's UTC_DATETIME parser uses
# strptime with %f, which only accepts microseconds (<=6 fractional digits).
# Truncate any longer fraction to 6 digits, leaving the timezone (Z / +HH:MM)
# untouched. Values without a sub-second fraction pass through unchanged.
_NANOS_RE = re.compile(r"(\.\d{6})\d+")


def _normalize_ts(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return _NANOS_RE.sub(r"\1", value, count=1)


def _map_contact(contact: dict) -> dict:
    """Flatten a contact into a warehouse row.

    The API returns the contact's fields under "fields", keyed by
    their common name; they are spread in first, and core columns are written
    afterwards so they always win on any key collision.
    """
    row: dict[str, Any] = {}
    for key, value in (contact.get("fields") or {}).items():
        row[key] = value

    row.update(
        {
            "id": contact.get("id"),
            "sfdc_lead_id": contact.get("sfdcLeadId"),
            "sfdc_contact_id": contact.get("sfdcContactId"),
            "sfdc_account_id": contact.get("sfdcAccountId"),
            "email": contact.get("email"),
            "subscription_status": contact.get("subscriptionStatus"),
            "created_at": _normalize_ts(contact.get("createdAt")),
            "updated_at": _normalize_ts(contact.get("updatedAt")),
        }
    )
    return row


def _map_email_event(event: dict) -> dict:
    """Map one email event onto its warehouse row."""
    return {
        "event_id": event.get("eventId"),
        "contact_id": event.get("contactId"),
        "occurred_at": _normalize_ts(event.get("occurredAt")),
        "event_type": event.get("eventType"),
        "source_type": event.get("sourceType"),
        "source_id": event.get("sourceId"),
        "email_id": event.get("emailId"),
        "email_name": event.get("emailName"),
        "sent_email_id": event.get("sentEmailId"),
        "is_bot": event.get("isBot"),
        "link": event.get("link"),
        "topic_ids": event.get("topicIds"),
        "bounce_type": event.get("bounceType"),
        "error_message": event.get("errorMessage"),
    }


def _map_custom_event(event: dict) -> dict:
    """Map one custom event onto its warehouse row.

    ``data`` is a free-form JSON object whose keys vary per event name, so it is
    stored in a single ``JSON`` column rather than flattened. It is passed
    through as the parsed object: the SDK serializes declared ``JSON`` columns
    itself (a pre-serialized string would be double-encoded).
    """
    return {
        "event_id": event.get("eventId"),
        "contact_id": event.get("contactId"),
        "occurred_at": _normalize_ts(event.get("occurredAt")),
        "created_at": _normalize_ts(event.get("createdAt")),
        "event_name": event.get("eventName"),
        "source": event.get("source"),
        "contact_email": event.get("contactEmail"),
        "user_id": event.get("userId"),
        "client_event_id": event.get("clientEventId"),
        "data": event.get("data"),
    }


# --------------------------------------------------------------------------- #
#                                  HTTP                                        #
# --------------------------------------------------------------------------- #
def _post(base_url: str, api_key: str, path: str, body: dict) -> dict:
    """POST to the public API and return the full response envelope.

    The envelope is ``{"data": {...}, "pagination": {"nextCursor": ...}}``;
    callers read rows from ``data`` and the next cursor from
    ``pagination.nextCursor``.

    Retries transient failures with exponential backoff. Raises on a structured
    API error or after exhausting retries.
    """
    url = base_url + path
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    last_err: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            # Retry server-side and rate-limit responses; fail fast on 4xx.
            if resp.status_code >= 500 or resp.status_code == 429:
                raise requests.HTTPError(f"{resp.status_code} from {path}: {resp.text[:500]}")
            if resp.status_code >= 400:
                raise RuntimeError(f"{resp.status_code} from {path}: {resp.text[:500]}")

            payload = resp.json()
            if isinstance(payload, dict) and payload.get("error"):
                raise RuntimeError(f"API error from {path}: {payload['error']}")
            return payload or {}
        except (requests.RequestException, requests.HTTPError) as err:
            last_err = err
            if attempt < MAX_RETRIES - 1:
                sleep_for = BACKOFF_BASE_SECONDS * (2**attempt)
                log.warning(f"{path}: request failed ({err}); retrying in {sleep_for}s")
                time.sleep(sleep_for)

    raise RuntimeError(f"{path}: request failed after {MAX_RETRIES} attempts: {last_err}")


def _require_config(configuration: dict) -> None:
    for key in ("base_url", "api_key"):
        if not configuration.get(key):
            raise ValueError(f"missing required configuration value: '{key}'")


# The connector object Fivetran loads.
connector = Connector(update=update, schema=schema)


# Local debugging entrypoint: `python connector.py` (or `fivetran debug`).
if __name__ == "__main__":
    import json

    with open("configuration.json") as f:
        connector.debug(configuration=json.load(f))
