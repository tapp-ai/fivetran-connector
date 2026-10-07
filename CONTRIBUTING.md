# Contributing

Thanks for your interest in improving the Conversion Fivetran connector!

## Development setup

Setup, tests, lint, and a local sync against the real API are in the
[README's Development section](README.md#development).

## Guidelines

- Keep `connector.py` self-contained and dependency-light. The Fivetran runtime
  pre-installs `fivetran_connector_sdk` and `requests`, so those do not belong in
  `[project].dependencies` in `pyproject.toml`.
- Add or update tests in `tests/` for any behavior change; `uv run pytest` must pass.
- Run `uv run ruff format .` and `uv run ruff check .` before opening a PR.
- Never commit `configuration.json` or any real API key.

## License

By contributing, you agree that your contributions will be licensed under the
Apache License 2.0.
