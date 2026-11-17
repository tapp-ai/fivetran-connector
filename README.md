# Conversion Fivetran connector

A [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk) connector
that exports [Conversion](https://conversion.ai) data into a destination
warehouse. Customers run it in their own Fivetran account to sync their
contacts, email events, custom events, contact events, assets (campaigns,
campaign types, workflows, audiences, and audience membership), and Mosaic
touchpoints. Licensed under
[Apache-2.0](LICENSE).

Each email table requests a single EMAIL_* type from
`POST /v2/exports/contact-events`; the destination table name is the lowercased
event type. Earlier versions read the deprecated `POST /v2/exports/email-events`;
see [Upgrading from the email events export](#upgrading-from-the-email-events-export).

Which tables sync is controlled by the `tables` configuration value (see
[Table selection](#table-selection)). The **Group** column is the name to list
there.


| Table                     | Group           | Source                                         | Primary key | Notes                                                                                                                                |
| ------------------------- | --------------- | ---------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `contacts`                | `contacts`      | `POST /v2/exports/contacts`, or `/v3` with `version` | `id`        | One row per contact. Every contact field is flattened in as its own column keyed by its common name (e.g. `first_name`, `owner_id`). |
| `custom_events`           | `custom_events` | `POST /v2/exports/custom-events`               | `event_id`  | One row per custom (behavioral) event. Opt-in: not synced unless listed in `tables`.                                                 |
| `email_send`              | `email`         | `POST /v2/exports/contact-events` (`EMAIL_SEND`) | `event_id`  | One row per send event.                                                                                                              |
| `email_delivery`          | `email`         | `…` (`EMAIL_DELIVERY`)                         | `event_id`  | The email was accepted by the recipient's mail server.                                                                               |
| `email_open`              | `email`         | `…` (`EMAIL_OPEN`)                             | `event_id`  | One row per open event.                                                                                                              |
| `email_click`             | `email`         | `…` (`EMAIL_CLICK`)                            | `event_id`  | Includes `link`, the clicked URL.                                                                                                    |
| `email_bounce`            | `email`         | `…` (`EMAIL_BOUNCE`)                           | `event_id`  | Hard (permanent) bounces.                                                                                                            |
| `email_soft_bounce`       | `email`         | `…` (`EMAIL_SOFT_BOUNCE`)                      | `event_id`  | Soft (transient) bounces.                                                                                                            |
| `email_complaint`         | `email`         | `…` (`EMAIL_COMPLAINT`)                        | `event_id`  | The recipient marked the email as spam.                                                                                              |
| `email_unsubscribe_all`   | `email`         | `…` (`EMAIL_UNSUBSCRIBE_ALL`)                  | `event_id`  | The recipient unsubscribed from all email.                                                                                           |
| `email_topic_unsubscribe` | `email`         | `…` (`EMAIL_TOPIC_UNSUBSCRIBE`)                | `event_id`  | The recipient unsubscribed from specific topics.                                                                                     |
| `page_visits`             | `contact_events` | `POST /v2/exports/contact-events` (`PAGE_VISIT`) | `event_id` | One row per page visit. Opt-in.                                                                                                     |
| `form_submissions`        | `contact_events` | `…` (`FORM_SUBMISSION`)                       | `event_id`  | One row per form submission. Opt-in.                                                                                                 |
| `campaign_member_events`  | `contact_events` | `…` (`CAMPAIGN_MEMBER`)                       | `event_id`  | Campaign membership adds, removals, status changes, and field changes. Opt-in.                                                     |
| `workflow_events`         | `contact_events` | `…` (`WORKFLOW`)                              | `event_id`  | Workflow entered, exited, and error events. Opt-in.                                                                                  |
| `audience_events`         | `contact_events` | `…` (`AUDIENCE`)                              | `event_id`  | Audience adds and removals. Opt-in.                                                                                                  |
| `field_changes`           | `contact_events` | `…` (`FIELD_CHANGE`)                          | `event_id`  | One row per contact field change. Opt-in.                                                                                            |
| `lead_conversions`        | `contact_events` | `…` (`SFDC_LEAD_CONVERTED`)                   | `event_id`  | Salesforce Lead to Contact conversions. Opt-in.                                                                                      |
| `campaigns`               | `assets`        | `POST /v2/exports/campaigns`                   | `id`        | Custom and blast campaigns, webinars, and field events. Opt-in snapshot.                                                            |
| `campaign_types`          | `assets`        | `POST /v2/exports/campaign-types`              | `id`        | Opt-in snapshot.                                                                                                                     |
| `campaign_type_statuses`  | `assets`        | `…` (each type's `statuses`)                   | `id`        | Member statuses per campaign type; joins `campaign_member_events.status_id`. Opt-in snapshot.                                       |
| `workflows`               | `assets`        | `POST /v2/exports/workflows`                   | `id`        | Opt-in snapshot.                                                                                                                     |
| `audiences`               | `assets`        | `POST /v2/exports/audiences`                   | `id`        | Opt-in snapshot.                                                                                                                     |
| `audience_members`        | `assets`        | `POST /v2/exports/audience-members`            | `audience_id`, `contact_id` | Current membership. Opt-in snapshot.                                                                                 |
| `touchpoints`             | `attribution`   | `POST /v2/exports/touchpoints`                 | `touchpoint_id` | Mosaic touchpoints. Opt-in; the business needs Mosaic enabled.                                                                   |

Email and custom events keep their own tables; the contact event tables don't
repeat them.

## Table selection

`tables` is an optional, comma-separated list of groups and/or table names. A
table that is not listed is neither declared in the schema nor synced, so it
never appears in the destination.


| `tables`                         | Syncs                                                     |
| -------------------------------- | --------------------------------------------------------- |
| *(unset)*                        | `contacts` and every `email_*` table (the original set).  |
| `custom_events`                  | Only `custom_events`.                                     |
| `contacts,email,custom_events`   | Everything.                                               |
| `contacts,email_click`           | Groups and single tables mix freely.                      |
| `contact_events,assets`          | Every contact event table and every asset table.          |
| `attribution`                    | Only `touchpoints`.                                       |


Unknown names fail the sync at startup. Fivetran's own schema tab still works as
a per-table opt-out on top of this. Changing `tables` after the first sync only
adds or removes tables going forward; trigger a resync if you need history for a
newly added table.


## API docs

This connector uses the Conversion [bulk export APIs](https://docs.conversion.ai/api-reference/export-contacts). Contact your Conversion account team for access to these APIs.

### Authentication

Requests carry the business's API key in the `X-API-Key` header
(`sk_live_<id>_<secret>`). The API scopes every response to the business that
owns the key, so the connector never sends a business id.

## Contact columns

Core columns are declared with fixed types:


| Name                  | Data type      | Description                                                                                                                                                                         |
| --------------------- | -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                  | `STRING`       | Contact primary key.                                                                                                                                                                |
| `sfdc_lead_id`        | `STRING`       | The contact's Salesforce ID when its Salesforce object type is a **Lead**; null otherwise.                                                                                          |
| `sfdc_contact_id`     | `STRING`       | The contact's Salesforce ID when its Salesforce object type is a **Contact**; null otherwise.                                                                                       |
| `sfdc_account_id`     | `STRING`       | The Salesforce Account ID of the contact's linked company (the company's Salesforce id), returned inline by the API; null when there's no company or it isn't synced to Salesforce. |
| `email`               | `STRING`       | Contact email address.                                                                                                                                                              |
| `subscription_status` | `STRING`       | Contact subscription status.                                                                                                                                                        |
| `created_at`          | `UTC_DATETIME` | When the contact was created.                                                                                                                                                       |
| `updated_at`          | `UTC_DATETIME` | When the contact was last updated.                                                                                                                                                  |


Every contact field is added as an extra column named by the field's key (its
"common name"). These columns are left undeclared so Fivetran infers them as
the set of fields grows.

## Email event columns

Every `email_*` table is declared with the same columns. The **Tables** column
lists which tables actually populate each column (`all` = every `email_*`
table): the shared columns are populated everywhere, while the
rest carry data only for the event types noted (e.g. `email_open` and
`email_delivery` carry only the shared columns).


| Name            | Data type      | Tables                              | Description                                                                                                                                                                                              |
| --------------- | -------------- | ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event_id`      | `STRING`       | all                                 | Event primary key.                                                                                                                                                                                       |
| `contact_id`    | `STRING`       | all                                 | The contact this event belongs to; joins `contacts.id`.                                                                                                                                                  |
| `occurred_at`   | `UTC_DATETIME` | all                                 | When the event occurred.                                                                                                                                                                                 |
| `event_type`    | `STRING`       | all                                 | The event type, matching the table: `EMAIL_SEND`, `EMAIL_OPEN`, `EMAIL_CLICK`, and so on.                                                                                                                             |
| `source_type`   | `STRING`       | all                                 | The kind of flow that sent the email: `WORKFLOW` or `BLAST`.                                                                                                                                             |
| `source_id`     | `STRING`       | all                                 | Id of the sending flow: either the `WORKFLOW` id or `BLAST` id.                                                                                                                                          |
| `email_id`      | `STRING`       | all                                 | Id of the email asset.                                                                                                                                                                                   |
| `email_name`    | `STRING`       | all                                 | Name of the email asset, resolved server-side.                                                                                                                                                           |
| `sent_email_id` | `STRING`       | all                                 | Per-send instance id.                                                                                                                                                                                    |
| `is_bot`        | `BOOLEAN`      | all                                 | Whether the engagement was bot-classified; filter in the warehouse as needed.                                                                                                                            |
| `link`          | `STRING`       | `email_click`                       | The clicked URL in the email.                                                                                                                                                                            |
| `topic_ids`     | `STRING`       | `email_topic_unsubscribe`           | Stringified array of topic ids the recipient unsubscribed from. Only populated if recipient unsubscribed from specific topics; if the user subscribed from all communication, no topic ids are returned. |
| `bounce_type`   | `STRING`       | `email_bounce`, `email_soft_bounce` | Bounce classification (`Permanent`, `Transient`, `Undetermined`).                                                                                                                                          |
| `error_message` | `STRING`       | `email_bounce`, `email_soft_bounce` | SMTP diagnostic code (e.g. `smtp; 550 5.1.1 user unknown`).                                                                                                                                               |


## Custom event columns


| Name              | Data type      | Description                                                                                                               |
| ----------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `event_id`        | `STRING`       | Event primary key.                                                                                                        |
| `contact_id`      | `STRING`       | The contact this event belongs to; joins `contacts.id`.                                                                   |
| `occurred_at`     | `UTC_DATETIME` | When the event occurred (caller-supplied; may be backdated).                                                              |
| `created_at`      | `UTC_DATETIME` | When Conversion recorded the event.                                                                                       |
| `event_name`      | `STRING`       | The custom event name, e.g. `signed_up`.                                                                                  |
| `source`          | `STRING`       | How the event was recorded (API, SDK, workflow, …).                                                                       |
| `contact_email`   | `STRING`       | The contact's **current** email, resolved at export time; null once the contact is deleted.                              |
| `user_id`         | `STRING`       | The contact's **current** external user id, resolved at export time; null once the contact is deleted.                   |
| `client_event_id` | `STRING`       | Caller-supplied idempotency id, when one was sent.                                                                        |
| `data`            | `JSON`         | The event payload object. Its keys vary per event name and are lowercased on ingestion; query it with your warehouse's JSON functions. |


## Contact event columns

Every contact event table has these columns:


| Name          | Data type      | Description                                                         |
| ------------- | -------------- | ------------------------------------------------------------------- |
| `event_id`    | `STRING`       | Event primary key.                                                  |
| `contact_id`  | `STRING`       | The contact this event belongs to; joins `contacts.id`.             |
| `event_type`  | `STRING`       | The specific event, e.g. `WORKFLOW_ENTERED` or `CAMPAIGN_MEMBER_STATUS_CHANGED`. |
| `occurred_at` | `UTC_DATETIME` | When the event occurred.                                            |
| `created_at`  | `UTC_DATETIME` | When Conversion recorded the event.                                 |
| `source_type` | `STRING`       | The flow that caused the event, e.g. `WORKFLOW` or `BLAST`.         |
| `source_id`   | `STRING`       | Id of that flow.                                                    |


Each table adds the columns of its family:


| Table                    | Columns                                                                                                                                                       |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `page_visits`            | `page_url`, `page_visit_type`, `sent_email_id` (set when the visit came from an email link), plus the web columns below                                        |
| `form_submissions`       | `form_id`, `page_url`, `fields` (`JSON`, the submitted values), plus the web columns below                                                                     |
| `campaign_member_events` | `campaign_id`, `campaign_member_id`, `campaign_type_id`, `campaign_category`, `status_id`, `old_status_id`, `is_success`, `old_is_success`, and `field_key`, `old_value`, `new_value` on `CAMPAIGN_MEMBER_FIELD_CHANGED` |
| `workflow_events`        | `workflow_id`, `workflow_run_id` (ties one run's events together), `workflow_node_id`                                                                          |
| `audience_events`        | `audience_id`                                                                                                                                                 |
| `field_changes`          | `field_key` (the contact field's key, as in the `contacts` columns), `old_value`, `new_value`                                                                  |
| `lead_conversions`       | `old_salesforce_lead_id`, `new_salesforce_contact_id`                                                                                                         |


The web columns are `session_id`, `tracking_id`, `referrer`, `utm_source`,
`utm_medium`, `utm_campaign`, `utm_term`, `utm_content`, `utm_extra` (`JSON`),
`landing_page_id`, `landing_page_route_id`, `landing_page_version_id`, `ad_id`,
`ad_group_id`, `ad_campaign_id`, `ad_account_id`, and `ad_keyword`.

## Asset columns


| Table                    | Columns                                                                                                                        |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `campaigns`              | `id`, `name`, `description`, `category` (`CUSTOM`, `BLAST`, `WEBINAR`, `FIELD_EVENT`), `campaign_type_id`, `archived`, `salesforce_campaign_id`, `created_at`, `updated_at` |
| `campaign_types`         | `id`, `name`, `description`, `category`, `archived`, `created_at`, `updated_at`                                                 |
| `campaign_type_statuses` | `id`, `campaign_type_id`, `name`, `position` (`INT`, 0-based order), `is_success`, `hidden`, `parent_status_id`, `created_at`, `updated_at` |
| `workflows`              | `id`, `name`, `status` (`ACTIVE`, `ACTIVATING`, `PAUSED`), `activated_at`, `created_at`, `updated_at`                          |
| `audiences`              | `id`, `name`, `type` (`STATIC`, `DYNAMIC`), `created_at`, `updated_at`                                                          |
| `audience_members`       | `audience_id`, `contact_id`, `created_at` (when the contact joined), `updated_at`                                               |


Ad campaigns are not exported. Deleted workflows are not returned, so they are
marked deleted on the next sync.

## Touchpoint columns

Touchpoints are computed under the business's Mosaic attribution settings when
they're exported, so a settings change (such as a channel mapping) reaches
already-synced rows only after a resync.

| Name | Data type | Description |
| --- | --- | --- |
| `touchpoint_id` | `STRING` | Touchpoint primary key; stable across syncs. |
| `contact_id` | `STRING` | The contact; joins `contacts.id`. |
| `touch_type` | `STRING` | `WEB_SESSION`, `EMAIL_CLICK`, `CAMPAIGN_SUCCESS`, or `LEAD_GEN_FORM_SUBMISSION`. |
| `occurred_at` | `UTC_DATETIME` | When the touch happened. |
| `session_id` | `STRING` | The web session, on web and email touches that landed one. |
| `form_id` | `STRING` | The form submitted in the session or on the ad platform. |
| `landing_page_url` | `STRING` | The first page of the session. |
| `referrer` | `STRING` | The referring URL. |
| `keyword` | `STRING` | The paid search keyword. |
| `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, `utm_content` | `STRING` | UTM parameters; empty on `CAMPAIGN_SUCCESS`. |
| `ad_id`, `ad_group_id`, `ad_campaign_id`, `ad_account_id` | `STRING` | The ad platform's own IDs. |
| `ad_provider` | `STRING` | `GOOGLE`, `META`, or `LINKEDIN`. |
| `campaign_id` | `STRING` | The campaign on `CAMPAIGN_SUCCESS`, the Conversion ad campaign on web and lead gen touches, and the blast on email touches. |
| `channel_id`, `subchannel_id` | `STRING` | The Mosaic channel and subchannel the touch was assigned to. |

## Upgrading from the email events export

The email tables now read the contact events export, whose cursors wrap their
filters and don't match the email events export's. On the first sync after
upgrading, each email table sends its old `<table>_cursor` once as
`emailEventsCursor`, and the export resumes where the email events walk
stopped. The table then keeps its cursor under `<table>_contact_events_cursor`
and drops the old key. Nothing is re-read, and the tables keep their rows and
columns.

The contact events export needs the API key to have both `view_contacts` and
`view_email_performance`; the email events export needed only
`view_email_performance`. A key that syncs only email tables and lacks
`view_contacts` gets `403` after upgrading until it is granted.

## Incremental sync

Each table keeps its own **opaque cursor** in connector `state`, keyed by table
(`contacts_cursor`, `email_send_contact_events_cursor`, `custom_events_cursor`, …). The
connector never interprets the cursor; it stores whatever the API last returned
and sends it back.

Every request posts `{"limit": 1000, "cursor": <saved cursor>}`. The email
tables send `{"types": [<EMAIL_* type>]}` on their first request, then only the
cursor. `custom_events` is a windowed export: its very
first request sends `{"occurredAt": {"start": <custom_events_start>}}` instead
of a cursor, and every later request sends only the cursor. No `end` is sent, so
the server re-resolves "now" on each request and the stored cursor keeps
advancing into new events on every sync. The response envelope is
`{"data": {…}, "pagination": {"nextCursor": …}}`; the connector reads rows from
`data` and the next cursor from `pagination.nextCursor`.

Paging is driven by `pagination.nextCursor`, **not** by page length: a short
page is **not** end-of-stream, so the connector keeps paging as long as the
cursor advances and stops only when it is exhausted (`null`) or stops advancing.
It checkpoints `state` after every page, so progress is durable and the next
sync resumes from the stored cursor. Rows are upserted by primary key
(`id` / `event_id`), so any row the API re-emits updates in place.

With `version` set to `v3`, `contacts` also deletes rows. The
v3 export lists live and deleted contacts in one `contacts` list, in the order
to apply them, and the connector deletes a row with `deleted: true` instead of
upserting it. A `contacts` walk with no
stored cursor (the first sync, or a resync of the table) truncates the table
first: the API replays every deletion it has recorded, but not those from
before it started recording them. A restored
contact comes back as an upsert. Each version keeps its own cursor
(`contacts_cursor`, `contacts_v3_cursor`). The v3 export accepts a v2 cursor, so
switching a connection to v3 resumes its walk without a truncate, and switching
back to v2 resumes the v2 cursor from where v3 took over.

The contact event tables work like `custom_events`: each table's first request
sends its family in `parentTypes` and `{"occurredAt": {"start": <contact_events_start>}}`,
and every later request sends only the stored cursor.

`touchpoints` also works like `custom_events`, with `touchpoints_start` as the
first-sync floor. The server holds the walk a day before now so sessions and
email clicks settle before their touchpoints export.

Asset tables are snapshots. Each sync truncates the table (Fivetran marks the
existing rows `_fivetran_deleted`), pages the export from no cursor to the end,
and upserts every row it returns. Rows the API no longer returns, such as a
contact that left an audience, stay marked deleted. No asset cursor is kept in
`state`. If a sync fails partway, the rows it hasn't re-read yet stay marked
deleted until the next sync finishes.

Network errors, 5xx and 429 responses are retried with exponential backoff, up
to four attempts. Other 4xx responses fail the sync immediately.

## Configuration

Copy the example and fill in a real key. `configuration.json` is gitignored so
the key is never committed:

```bash
cp configuration.example.json configuration.json
# then edit configuration.json:
# {
#   "base_url": "https://pub-api.conversion.ai/api",
#   "api_key": "sk_live_<id>_<secret>",
#   "tables": "contacts,email",                    # optional; see Table selection
#   "custom_events_start": "2020-01-01T00:00:00Z", # optional; first-sync floor for custom_events
#   "contact_events_start": "2020-01-01T00:00:00Z", # optional; first-sync floor for the contact event tables
#   "touchpoints_start": "2020-01-01T00:00:00Z",    # optional; first-sync floor for touchpoints
#   "version": "v2"                                 # optional; v3 also syncs deleted contacts
# }
```

All values are strings (a Fivetran Connector SDK requirement). After deploy the
same values are editable in the connection's setup form in the Fivetran
dashboard.

`custom_events_start` (RFC-3339 UTC, default `2020-01-01T00:00:00Z`) is the
earliest `occurred_at` the first `custom_events` sync backfills. It is only
read when no cursor exists yet, so changing it on an existing connection has no
effect until you resync the connector. `contact_events_start` and
`touchpoints_start` do the same for the contact event tables and touchpoints.

`version` (`v2` or `v3`, default `v2`) picks the API version the connector
reads. Only `contacts` differs: `v3` also deletes contacts deleted in Conversion
(see Incremental sync). It rolls out one connection at a time, so `v2` stays
the default.

## Development

This project uses [uv](https://docs.astral.sh/uv/). From this directory:

```bash
uv sync --extra dev          # create the venv and install deps (incl. dev tools)
uv run pytest                # run the tests
uv run ruff check .          # lint
uv run ruff format --check . # check formatting
```

To sync against the real API into a local DuckDB warehouse, so you can inspect
tables and confirm cursors advance:

```bash
uv run fivetran debug --configuration configuration.json
```

The Fivetran runtime pre-installs `fivetran_connector_sdk` and `requests`.
Fivetran reads `pyproject.toml` at deploy time (it takes precedence over
`requirements.txt`) and installs `[project].dependencies`, which is
intentionally empty: declaring the pre-installed packages breaks the deploy. The
SDK, `requests`, and test and lint tools live in the `dev` extra, so they are
installed locally but never by Fivetran.

## Deploy

```bash
uv run fivetran deploy --api-key <FIVETRAN_DEPLOY_KEY> --destination <DEST> --connection conversion
```

## Maintaining

This directory is developed in Conversion's monorepo and mirrored to the root
of the public `tapp-ai/fivetran-connector` repo on every push to `main` that
touches it, so everything here is public. The connector is not part of the
monorepo's Bazel build or CI; `.github/workflows/ci.yml` runs the checks above
in the public repo after the mirror.

The connector reads the export responses of Conversion's public API field by
field. When an export's shape changes, update the API and this connector in
the same change, and keep this README in step with the connector.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). All contributions are licensed under
Apache-2.0.
