# Contributing

Thanks for your interest in improving the Conversion Fivetran connector!

## Development setup

This project uses [uv](https://docs.astral.sh/uv/).

```bash
uv sync                 # create the venv and install deps (incl. dev tools)
uv run pytest           # run the test suite
uv run ruff check .     # lint
uv run ruff format .    # format
```

## Running the connector locally

```bash
cp configuration.example.json configuration.json   # then add your API key
uv run fivetran debug --configuration configuration.json
```

## Guidelines

- Keep `connector.py` self-contained and dependency-light — the Fivetran runtime
  pre-installs `fivetran_connector_sdk` and `requests`, so those do not belong in
  `requirements.txt`.
- Add or update tests in `tests/` for any behavior change; `uv run pytest` must pass.
- Run `uv run ruff format .` and `uv run ruff check .` before opening a PR.
- Never commit `configuration.json` or any real API key.

## License

By contributing, you agree that your contributions will be licensed under the
Apache License 2.0.
