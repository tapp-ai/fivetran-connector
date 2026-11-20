r"""Fivetran Connector SDK connector for Conversion.

Exports tables from the Conversion public API into a destination warehouse:

  - contacts   one row per contact, with every contact field
               flattened in as a column keyed by its common name (e.g. owner_id,
               first_name, ...).
  - nine per-event email tables (email_send, email_delivery, email_open,
    email_click, email_bounce, email_soft_bounce, email_complaint,
    email_unsubscribe_all, email_topic_unsubscribe), each one row per email
    engagement event of a single EMAIL_* type, read from the contact events
    export. See EMAIL_STREAMS for the table -> event type mapping.
  - custom_events  one row per custom (behavioral) event recorded against a
               contact. Opt-in: see "Table selection" below.
  - seven contact event tables, one per event family (page_visits,
    form_submissions, campaign_member_events, workflow_events, audience_events,
    field_changes, lead_conversions). Opt-in. See CONTACT_EVENT_STREAMS.
  - asset tables (campaigns, campaign_types, campaign_type_statuses, workflows,
    audiences, audience_members). Opt-in. Each sync re-reads them in full; see
    ASSET_STREAMS.
  - touchpoints  one row per Mosaic touchpoint. Opt-in, and the business needs
               Mosaic enabled.

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

The contact event tables and ``touchpoints`` walk the same way: each sends its event family and
``occurredAt.start`` on the first request only, then just the cursor.

Asset tables are snapshots rather than streams. Every sync marks the table's
existing rows deleted (``truncate``), then pages from no cursor to the end and
upserts what the API returns, so deleted assets and removed audience members
drop out. No asset cursor is kept in state.

Table selection
---------------
The optional ``tables`` configuration value is a comma-separated list of table
names and/or groups (``contacts``, ``email``, ``custom_events``,
``contact_events``, ``assets``, ``attribution``; ``email`` expands to every
``email_*`` table).
Unlisted tables are neither declared in the schema nor synced. When ``tables``
is unset the connector syncs ``contacts`` and ``email`` — exactly what it synced
before this option existed — so existing connections are unaffected by a
redeploy.
"""

from __future__ import annotations

import json
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

# The per-event email tables and the EMAIL_* contact event type each one
# requests from the contact events export. The destination table name is the
# lowercased event type.
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

# Default floor for the custom_events walk, overridable with the
# `custom_events_start` configuration value (RFC-3339 UTC). Sent only on the
# very first request (before any cursor exists); afterwards the cursor carries
# the range, so changing it later has no effect until the connector is resynced.
# The server bounds each internal query, so a long range is safe.
CUSTOM_EVENTS_START = "2020-01-01T00:00:00Z"
_RFC3339_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def _resolve_start(configuration: dict, key: str) -> str:
    """Return a first-sync floor from ``configuration[key]``, validating the format."""
    raw = (configuration.get(key) or CUSTOM_EVENTS_START).strip()
    if not _RFC3339_UTC_RE.match(raw):
        raise ValueError(
            f"configuration '{key}' must be an RFC-3339 UTC timestamp "
            f"like 2024-01-01T00:00:00Z, got '{raw}'"
        )
    return raw


def _resolve_custom_events_start(configuration: dict) -> str:
    """Return the first-sync floor for custom_events."""
    return _resolve_start(configuration, "custom_events_start")


def _resolve_contact_events_start(configuration: dict) -> str:
    """Return the first-sync floor for the contact event tables."""
    return _resolve_start(configuration, "contact_events_start")


def _resolve_touchpoints_start(configuration: dict) -> str:
    """Return the first-sync floor for touchpoints."""
    return _resolve_start(configuration, "touchpoints_start")


# Which API version the connector reads, from the `version` configuration
# value. Only `contacts` differs: v2 lists live contacts only; v3 also lists
# deleted contacts. v2 stays the default while v3 rolls out one connection at a
# time.
VERSIONS = ("v2", "v3")


def _resolve_version(configuration: dict) -> str:
    """Return the API version, validating it."""
    raw = (configuration.get("version") or "v2").strip().lower()
    if raw not in VERSIONS:
        raise ValueError(f"configuration 'version' must be v2 or v3, got '{raw}'")
    return raw


# Mosaic touchpoint columns, read from each exported touchpoint.
TOUCHPOINT_COLUMNS: dict[str, str] = {
    "touchpoint_id": "STRING",
    "contact_id": "STRING",
    "touch_type": "STRING",
    "occurred_at": "UTC_DATETIME",
    "session_id": "STRING",
    "form_id": "STRING",
    "landing_page_url": "STRING",
    "referrer": "STRING",
    "keyword": "STRING",
    "utm_source": "STRING",
    "utm_medium": "STRING",
    "utm_campaign": "STRING",
    "utm_term": "STRING",
    "utm_content": "STRING",
    "ad_id": "STRING",
    "ad_group_id": "STRING",
    "ad_campaign_id": "STRING",
    "ad_account_id": "STRING",
    "ad_provider": "STRING",
    "campaign_id": "STRING",
    "channel_id": "STRING",
    "subchannel_id": "STRING",
}


# Columns shared by every contact event table, read from the event's top level.
CONTACT_EVENT_COLUMNS: dict[str, str] = {
    "event_id": "STRING",
    "contact_id": "STRING",
    "event_type": "STRING",
    "occurred_at": "UTC_DATETIME",
    "created_at": "UTC_DATETIME",
    "source_type": "STRING",
    "source_id": "STRING",
}

# Session and attribution columns on page visits and form submissions.
WEB_COLUMNS: dict[str, str] = {
    "session_id": "STRING",
    "tracking_id": "STRING",
    "referrer": "STRING",
    "utm_source": "STRING",
    "utm_medium": "STRING",
    "utm_campaign": "STRING",
    "utm_term": "STRING",
    "utm_content": "STRING",
    "utm_extra": "JSON",
    "landing_page_id": "STRING",
    "landing_page_route_id": "STRING",
    "landing_page_version_id": "STRING",
    "ad_id": "STRING",
    "ad_group_id": "STRING",
    "ad_campaign_id": "STRING",
    "ad_account_id": "STRING",
    "ad_keyword": "STRING",
}

# (table, parentType sent on the first request, family object key in each
# event, family columns). Family columns are read from that object.
CONTACT_EVENT_STREAMS: list[tuple[str, str, str, dict[str, str]]] = [
    (
        "page_visits",
        "PAGE_VISIT",
        "pageVisit",
        {
            "page_url": "STRING",
            "page_visit_type": "STRING",
            "sent_email_id": "STRING",
            **WEB_COLUMNS,
        },
    ),
    (
        "form_submissions",
        "FORM_SUBMISSION",
        "formSubmission",
        {"form_id": "STRING", "page_url": "STRING", "fields": "JSON", **WEB_COLUMNS},
    ),
    (
        "campaign_member_events",
        "CAMPAIGN_MEMBER",
        "campaignMember",
        {
            "campaign_id": "STRING",
            "campaign_member_id": "STRING",
            "campaign_type_id": "STRING",
            "campaign_category": "STRING",
            "status_id": "STRING",
            "old_status_id": "STRING",
            "is_success": "BOOLEAN",
            "old_is_success": "BOOLEAN",
            "field_key": "STRING",
            "old_value": "STRING",
            "new_value": "STRING",
        },
    ),
    (
        "workflow_events",
        "WORKFLOW",
        "workflow",
        {"workflow_id": "STRING", "workflow_run_id": "STRING", "workflow_node_id": "STRING"},
    ),
    ("audience_events", "AUDIENCE", "audience", {"audience_id": "STRING"}),
    (
        "field_changes",
        "FIELD_CHANGE",
        "fieldChange",
        {"field_key": "STRING", "old_value": "STRING", "new_value": "STRING"},
    ),
    (
        "lead_conversions",
        "SFDC_LEAD_CONVERTED",
        "leadConversion",
        {},
    ),
]

_ASSET_TIMESTAMPS = {"created_at": "UTC_DATETIME", "updated_at": "UTC_DATETIME"}

# Asset snapshot tables: table -> (primary key, columns). campaign_type_statuses
# is filled from the statuses nested in each campaign type.
ASSET_TABLES: dict[str, tuple[list[str], dict[str, str]]] = {
    "campaigns": (
        ["id"],
        {
            "id": "STRING",
            "name": "STRING",
            "description": "STRING",
            "category": "STRING",
            "campaign_type_id": "STRING",
            "archived": "BOOLEAN",
            "salesforce_campaign_id": "STRING",
            **_ASSET_TIMESTAMPS,
        },
    ),
    "campaign_types": (
        ["id"],
        {
            "id": "STRING",
            "name": "STRING",
            "description": "STRING",
            "category": "STRING",
            "archived": "BOOLEAN",
            **_ASSET_TIMESTAMPS,
        },
    ),
    "campaign_type_statuses": (
        ["id"],
        {
            "id": "STRING",
            "campaign_type_id": "STRING",
            "name": "STRING",
            "position": "INT",
            "is_success": "BOOLEAN",
            "hidden": "BOOLEAN",
            "parent_status_id": "STRING",
            **_ASSET_TIMESTAMPS,
        },
    ),
    "workflows": (
        ["id"],
        {
            "id": "STRING",
            "name": "STRING",
            "status": "STRING",
            "activated_at": "UTC_DATETIME",
            **_ASSET_TIMESTAMPS,
        },
    ),
    "audiences": (
        ["id"],
        {"id": "STRING", "name": "STRING", "type": "STRING", **_ASSET_TIMESTAMPS},
    ),
    "audience_members": (
        ["audience_id", "contact_id"],
        {"audience_id": "STRING", "contact_id": "STRING", **_ASSET_TIMESTAMPS},
    ),
}

# (tables written, endpoint, rows key). campaign-types fills two tables.
ASSET_STREAMS: list[tuple[list[str], str, str]] = [
    (["campaigns"], "/v2/exports/campaigns", "campaigns"),
    (["campaign_types", "campaign_type_statuses"], "/v2/exports/campaign-types", "campaignTypes"),
    (["workflows"], "/v2/exports/workflows", "workflows"),
    (["audiences"], "/v2/exports/audiences", "audiences"),
    (["audience_members"], "/v2/exports/audience-members", "members"),
]


# Table groups accepted in the `tables` configuration value. Individual table
# names are also accepted.
TABLE_GROUPS: dict[str, list[str]] = {
    "contacts": ["contacts"],
    "email": [table for table, _ in EMAIL_STREAMS],
    "custom_events": ["custom_events"],
    "contact_events": [table for table, _, _, _ in CONTACT_EVENT_STREAMS],
    "assets": list(ASSET_TABLES),
    "attribution": ["touchpoints"],
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
    # Validate up front so a bad value fails at schema time, before any sync.
    _resolve_custom_events_start(configuration)
    _resolve_contact_events_start(configuration)
    _resolve_touchpoints_start(configuration)
    _resolve_version(configuration)

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
                    "user_id": "STRING",
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
        + [
            {
                "table": table,
                "primary_key": ["event_id"],
                "columns": {**CONTACT_EVENT_COLUMNS, **family_columns},
            }
            for table, _parent_type, _family_key, family_columns in CONTACT_EVENT_STREAMS
        ]
        + [
            {"table": table, "primary_key": primary_key, "columns": columns}
            for table, (primary_key, columns) in ASSET_TABLES.items()
        ]
        + [
            {
                "table": "touchpoints",
                "primary_key": ["touchpoint_id"],
                "columns": TOUCHPOINT_COLUMNS,
            }
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
    # v2 upserts live contacts. v3 also deletes deleted contacts, and a new walk
    # truncates the table. Each version keeps its own cursor, since v2 can't read
    # a v3 cursor: v3 starts from the v2 cursor, which it accepts, and switching
    # back to v2 resumes the v2 cursor from where v3 took over.
    if "contacts" in enabled:
        if _resolve_version(configuration) == "v3":
            if state.get("contacts_cursor") and not state.get("contacts_v3_cursor"):
                state["contacts_v3_cursor"] = state["contacts_cursor"]
            yield from _sync_stream(
                base_url=base_url,
                api_key=api_key,
                table="contacts",
                path="/v3/exports/contacts",
                body_extra={},
                rows_key="contacts",
                state=state,
                state_key="contacts_v3_cursor",
                row_mapper=_map_contact,
                deleted_key="deleted",
                truncate_new_walk=True,
            )
        else:
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
    # Read from the contact events export, one type per table. Its cursors wrap
    # their filters, so they live under a new state key. A table upgraded from
    # the deprecated email events export hands its old `<table>_cursor` over
    # once as emailEventsCursor, and the walk resumes where it stopped instead
    # of re-reading all history. The old key is dropped only once the new walk
    # has a cursor of its own, so a failed first page retries the handoff.
    for table, event_type in EMAIL_STREAMS:
        if table not in enabled:
            continue
        legacy_key = f"{table}_cursor"
        state_key = f"{table}_contact_events_cursor"
        first_page: dict[str, Any] = {"types": [event_type]}
        if state.get(legacy_key) and not state.get(state_key):
            first_page["emailEventsCursor"] = state[legacy_key]
        yield from _sync_stream(
            base_url=base_url,
            api_key=api_key,
            table=table,
            path="/v2/exports/contact-events",
            body_extra={},
            rows_key="events",
            state=state,
            state_key=state_key,
            row_mapper=_map_email_event,
            first_page_extra=first_page,
        )
        if state.get(state_key):
            state.pop(legacy_key, None)

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
            first_page_extra={"occurredAt": {"start": _resolve_custom_events_start(configuration)}},
        )

    # Contact events ---------------------------------------------------------
    # One table per event family; the family and occurredAt.start go on the
    # first request only, after which the cursor carries them.
    for table, parent_type, family_key, family_columns in CONTACT_EVENT_STREAMS:
        if table not in enabled:
            continue
        yield from _sync_stream(
            base_url=base_url,
            api_key=api_key,
            table=table,
            path="/v2/exports/contact-events",
            body_extra={},
            rows_key="events",
            state=state,
            state_key=f"{table}_cursor",
            row_mapper=_contact_event_mapper(family_key, family_columns),
            first_page_extra={
                "parentTypes": [parent_type],
                "occurredAt": {"start": _resolve_contact_events_start(configuration)},
            },
        )

    # Touchpoints ------------------------------------------------------------
    # Windowed like custom_events; the server holds the walk a day short of now
    # so sessions settle before their touchpoints export.
    if "touchpoints" in enabled:
        yield from _sync_stream(
            base_url=base_url,
            api_key=api_key,
            table="touchpoints",
            path="/v2/exports/touchpoints",
            body_extra={},
            rows_key="touchpoints",
            state=state,
            state_key="touchpoints_cursor",
            row_mapper=lambda touchpoint: _pick(touchpoint, TOUCHPOINT_COLUMNS),
            first_page_extra={"occurredAt": {"start": _resolve_touchpoints_start(configuration)}},
        )

    # Assets -----------------------------------------------------------------
    for tables, path, rows_key in ASSET_STREAMS:
        if any(table in enabled for table in tables):
            yield from _sync_snapshot(base_url, api_key, tables, enabled, path, rows_key, state)

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
    deleted_key: str | None = None,
    truncate_new_walk: bool = False,
) -> Iterable[Any]:
    """Page one table to exhaustion, upserting rows and checkpointing cursors.

    ``body_extra`` is sent on every request; ``first_page_extra`` only when no
    cursor exists yet (for endpoints whose cursor carries the initial range).

    ``deleted_key`` names the flag that marks a deleted row, which is deleted
    instead of upserted. Rows apply in page order, since the API lists an id's
    later state after its earlier one. With ``truncate_new_walk``, a walk with no
    stored cursor empties the table once its first page arrives: the API reports
    only the deletions it still keeps, so rows left from before (such as after a
    re-sync) would otherwise never be removed.

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

    truncate_pending = truncate_new_walk and not cursor
    restarted = False

    while True:
        body: dict[str, Any] = {"limit": PAGE_LIMIT, **body_extra}
        if cursor:
            body["cursor"] = cursor
        elif first_page_extra:
            body.update(first_page_extra)

        try:
            payload = _post(base_url, api_key, path, body)
        except CursorExpiredError:
            # One restart per sync: a fresh walk's cursor expiring again is a server fault.
            if not cursor or restarted:
                raise
            restarted = True
            # The stored cursor outlived what the API keeps; the walk restarts as a new one.
            log.warning(f"{table}: stored cursor expired; restarting the walk")
            cursor = None
            state.pop(state_key, None)
            truncate_pending = truncate_new_walk
            continue
        data = payload.get("data") or {}
        rows = data.get(rows_key) or []

        if truncate_pending:
            yield op.truncate(table)
            truncate_pending = False
        for raw in rows:
            if deleted_key and raw.get(deleted_key):
                yield op.delete(table=table, keys={"id": raw["id"]})
            else:
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


def _sync_snapshot(
    base_url: str,
    api_key: str,
    tables: list[str],
    enabled: set[str],
    path: str,
    rows_key: str,
    state: dict,
) -> Iterable[Any]:
    """Re-read one asset export in full and replace its enabled tables' rows.

    ``tables[0]`` is the export's own table; the rest are filled from it. The
    enabled tables are truncated first (a soft delete), so anything the API no
    longer returns stays marked deleted once the walk finishes. Assets are small,
    so each sync walks from no cursor to the end; no cursor goes into state.
    """
    counts = {table: 0 for table in tables if table in enabled}
    for table in counts:
        yield op.truncate(table)

    cursor: str | None = None
    while True:
        body: dict[str, Any] = {"limit": PAGE_LIMIT}
        if cursor:
            body["cursor"] = cursor
        payload = _post(base_url, api_key, path, body)
        for raw in (payload.get("data") or {}).get(rows_key) or []:
            for table, row in _map_asset(tables[0], raw):
                if table in counts:
                    counts[table] += 1
                    yield op.upsert(table=table, data=row)
        yield op.checkpoint(state)

        next_cursor = (payload.get("pagination") or {}).get("nextCursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    for table, count in counts.items():
        log.info(f"{table}: synced {count} rows")


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
    """Map one EMAIL contact event onto its email table row.

    ``topic_ids`` stays a compact JSON array string, the column's original shape.
    """
    email = event.get("email") or {}
    topic_ids = email.get("topicIds")
    return {
        "event_id": event.get("eventId"),
        "contact_id": event.get("contactId"),
        "user_id": email.get("userId"),
        "occurred_at": _normalize_ts(event.get("occurredAt")),
        "event_type": event.get("eventType"),
        "source_type": event.get("sourceType"),
        "source_id": event.get("sourceId"),
        "email_id": email.get("emailId"),
        "email_name": email.get("emailName"),
        "sent_email_id": email.get("sentEmailId"),
        "is_bot": email.get("isBot"),
        "link": email.get("link"),
        "topic_ids": json.dumps(topic_ids, separators=(",", ":")) if topic_ids else None,
        "bounce_type": email.get("bounceType"),
        "error_message": email.get("errorMessage"),
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


def _camel(column: str) -> str:
    """Return the API's camelCase key for a snake_case column."""
    head, *rest = column.split("_")
    return head + "".join(part.capitalize() for part in rest)


def _pick(source: dict, columns: dict[str, str]) -> dict:
    """Read each column from ``source`` by its camelCase key."""
    row: dict[str, Any] = {}
    for column, column_type in columns.items():
        value = source.get(_camel(column))
        row[column] = _normalize_ts(value) if column_type == "UTC_DATETIME" else value
    return row


def _contact_event_mapper(
    family_key: str, family_columns: dict[str, str]
) -> Callable[[dict], dict]:
    """Return a mapper that flattens one event family into its table's row."""

    def _map(event: dict) -> dict:
        row = _pick(event, CONTACT_EVENT_COLUMNS)
        row.update(_pick(event.get(family_key) or {}, family_columns))
        return row

    return _map


def _map_asset(table: str, raw: dict) -> list[tuple[str, dict]]:
    """Map one exported asset onto ``(table, row)`` pairs.

    A campaign type also yields one ``campaign_type_statuses`` row per status.
    """
    rows = [(table, _pick(raw, ASSET_TABLES[table][1]))]
    if table == "campaign_types":
        status_columns = ASSET_TABLES["campaign_type_statuses"][1]
        for status in raw.get("statuses") or []:
            row = _pick(status, status_columns)
            row["campaign_type_id"] = raw.get("id")
            rows.append(("campaign_type_statuses", row))
    return rows


# --------------------------------------------------------------------------- #
#                                  HTTP                                        #
# --------------------------------------------------------------------------- #
class CursorExpiredError(RuntimeError):
    """The API no longer keeps what a stored cursor needs, so its walk restarts."""


def _error_code(resp: Any) -> str | None:
    """Return the structured error code of a failed response, if it has one."""
    try:
        payload = resp.json()
    except ValueError:
        return None
    error = payload.get("error") if isinstance(payload, dict) else None
    return error.get("code") if isinstance(error, dict) else None


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
                if _error_code(resp) == "cursor_expired":
                    raise CursorExpiredError(f"{path}: cursor expired")
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
