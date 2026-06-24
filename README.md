# Conversion Fivetran connector

A [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk) connector
that exports [Conversion](https://conversion.ai) data into a destination
warehouse. Licensed under [Apache-2.0](LICENSE).

Each email table requests a single `eventType` from `POST /v2/exports/email-events`; the destination table name is the lowercased event type.

| Table | Source | Primary key | Notes |
|---|---|---|---|
| `contacts` | `POST /v2/exports/contacts` | `id` | One row per contact (lead). Every contact **variable schema** is flattened in as a column keyed by its common name — e.g. `owner_id`, `first_name`. |
| `email_send` | `POST /v2/exports/email-events` (`EMAIL_SEND`) | `event_id` | One row per send event. |
| `email_delivery` | `…` (`EMAIL_DELIVERY`) | `event_id` | Accepted by the recipient's mail server. |
| `email_open` | `…` (`EMAIL_OPEN`) | `event_id` | |
| `email_click` | `…` (`EMAIL_CLICK`) | `event_id` | Includes `link` (clicked URL). |
| `email_bounce` | `…` (`EMAIL_BOUNCE`) | `event_id` | Hard (permanent) bounces; `bounce_type` / `error_message` carry the detail. |
| `email_soft_bounce` | `…` (`EMAIL_SOFT_BOUNCE`) | `event_id` | Soft (transient) bounces. |
| `email_complaint` | `…` (`EMAIL_COMPLAINT`) | `event_id` | Marked as spam by the recipient. |
| `email_subscription` | `…` (`EMAIL_SUBSCRIPTION`) | `event_id` | (Re-)subscribed; `topic_ids` holds the scope. |
| `email_unsubscribe_all` | `…` (`EMAIL_UNSUBSCRIBE_ALL`) | `event_id` | Unsubscribed from all email. |
| `email_topic_unsubscribe` | `…` (`EMAIL_TOPIC_UNSUBSCRIBE`) | `event_id` | Unsubscribed from specific topics; `topic_ids` holds the scope. |

## API docs

This connector uses the Conversion [bulk export APIs](https://docs.conversion.ai/api-reference/export-contacts). Contact your Conversion account team for access to these APIs.

### Authentication

Requests carry the business's API key in the `X-API-Key` header
(`sk_live_<id>_<secret>`). The API scopes every response to the business that
owns the key, so the connector never sends a business id.

## Contact columns

Core columns are typed in `schema()`:


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


Every field schema value is added as an extra column named by the field's
key (its "common name"). These are left undeclared in `schema()` so Fivetran infers them automatically as the set of fields grows.

## Email event columns

### Shared columns

Shared across the `email_`* tables and typed in `schema()`:


| Name            | Data type      | Description                                                                   |
| --------------- | -------------- | ----------------------------------------------------------------------------- |
| `event_id`      | `STRING`       | Event primary key.                                                            |
| `contact_id`    | `STRING`       | The contact this event belongs to; joins `contacts.id`.                       |
| `occurred_at`   | `UTC_DATETIME` | When the event occurred.                                                      |
| `event_type`    | `STRING`       | The kind of event: `SEND`, `OPEN`, `CLICK`, `DELIVERED`, or `UNSUBSCRIBE`.    |
| `source_type`   | `STRING`       | The kind of flow that sent the email: `WORKFLOW` or `BLAST`.                  |
| `source_id`     | `STRING`       | Id of the sending flow: either the `WORKFLOW` id or `BLAST` id.               |
| `email_id`      | `STRING`       | Id of the email asset.                                                        |
| `email_name`    | `STRING`       | Name of the email asset, resolved server-side.                                |
| `sent_email_id` | `STRING`       | Per-send instance id.                                                         |
| `is_bot`        | `BOOLEAN`      | Whether the engagement was bot-classified; filter in the warehouse as needed. |


### Table-specific columns

These columns are declared on every `email_*` table but only populated for the
event type noted below. `email_open` and `email_delivered` carry only the
shared columns above.

#### `email_click`


| Name   | Data type | Description                   |
| ------ | --------- | ----------------------------- |
| `link` | `STRING`  | The clicked URL in the email. |


#### `email_unsubscribe`


| Name        | Data type | Description                                                                                                                                                                                              |
| ----------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `topic_ids` | `STRING`  | Stringified array of topic ids the recipient unsubscribed from.Only populated if recipient unsubscribed from specific topics; if the user subscribed from all communication, no topic ids are returned. |


#### `email_send`


| Name            | Data type | Description                                                                                                     |
| --------------- | --------- | --------------------------------------------------------------------------------------------------------------- |
| `bounce_type`   | `STRING`  | Bounce classification ("Permanent," "Transient," "Undetermined"); only populated for entries that were bounces. |
| `error_message` | `STRING`  | SMTP diagnostic code (e.g. `smtp; 550 5.1.1 user unknown`); only populated for entries that were bounces.       |


## Incremental sync

Each table keeps its own **opaque cursor** in connector `state`, keyed by table
(`contacts_cursor`, `email_send_cursor`, …). The connector never interprets the
cursor; it just stores whatever the API last returned and sends it back.

Every request posts `{"limit": 1000, "cursor": <saved cursor>}` (the email
tables also send `eventType`). The response envelope is
`{"data": {…}, "pagination": {"nextCursor": …}}`; the connector reads rows from
`data` and the next cursor from `pagination.nextCursor`.

Paging is driven by `pagination.nextCursor`, **not** by page length: a short
page is **not** end-of-stream, so the connector keeps paging as long as the
cursor advances and stops only when it is exhausted (`null`) or stops advancing.
It checkpoints `state` after every page, so progress is durable and the next
sync resumes from the stored cursor. Rows are upserted by primary key
(`id` / `event_id`), so any row the API re-emits updates in place.

## Configuration

Copy the example and fill in a real key. `configuration.json` is gitignored so
the key is never committed:

```bash
cp configuration.example.json configuration.json
# then edit configuration.json:
# {
#   "base_url": "https://pub-api.conversion.ai/api",
#   "api_key": "sk_live_<id>_<secret>"
# }
```

## Development

This project uses [uv](https://docs.astral.sh/uv/).

```bash
uv sync --extra dev     # create the venv and install deps (incl. dev tools)
uv run pytest           # run the tests
uv run ruff check .     # lint
uv run ruff format .    # format
```

The Fivetran runtime pre-installs `fivetran_connector_sdk` and `requests`, so
the connector needs no extra runtime dependencies. Fivetran reads
`pyproject.toml` at deploy time (it takes precedence over `requirements.txt`)
and installs `[project].dependencies`, which is intentionally empty. The SDK,
`requests`, and test/lint tooling live under the `dev` optional-dependencies
extra so they are installed locally but never by Fivetran.

## Run locally

```bash
# Debug against the API (reads configuration.json): runs the sync against a
# local DuckDB warehouse so you can inspect tables and confirm cursors advance:
uv run fivetran debug --configuration configuration.json
```

## Deploy

```bash
uv run fivetran deploy --api-key <FIVETRAN_DEPLOY_KEY> --destination <DEST> --connection conversion
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). All contributions are licensed under
Apache-2.0.