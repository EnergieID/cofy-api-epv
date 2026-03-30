# EPV Cofy API

This is the [EPV](https://www.enr-citoyennes.fr/) specific instance of the [cofy-api](https://github.com/EnergieID/cofy-api) — the open-source modular framework by [EnergyID](https://www.energieid.be/) for ingesting, standardising, and serving energy-related data.

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (fast Python package manager)
- [poethepoet](https://poethepoet.natn.io/index.html) (task runner `uv tool install poethepoet`)

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in your values
```

### 3. Start and seed the local Postgres database

The directive source expects a Postgres connection string in `DB_URL`. You can use any Postgres instance, but for local development we recommend the provided Docker setup. Spin up the container and seed the database with sample data:

```bash
poe db-reset
```

If you have your dev database running without docker, you can still use the seeding command to create the `history` table and load the sample data:

```bash
poe db-seed
```

### 4. Enable your modules

Open `main.py` and add the modules you need (tariff, production, …).
See [cofy-api README](https://github.com/EnergieID/cofy-api) for all available modules & options.

### 5. Run the dev server

```bash
poe dev          # starts FastAPI with auto-reload, reads .env
```

The API is now available at `http://localhost:8000`.
Health-check: `GET /health`

### 6. Run a production-like container locally

```bash
poe prod         # builds the Docker image and runs it on port 8080
```

The container is available at `http://localhost:8080`.
Health-check: `GET /health`

## Development

### Code quality (pre-commit)

Pre-commit hooks are configured out of the box. Install them once:

```bash
pre-commit install
```

Every commit will automatically run:

| Tool | Task | Command |
|------|------|---------|
| **Ruff** | Linting + auto-fix | `poe lint` |
| **Ruff** | Formatting | `poe format` |
| **ty** | Type checking | `poe check` |

You can also run them manually at any time.

### Local database workflow

```bash
poe db-up        # start the local postgres container
poe db-seed      # recreate the history table and load db/dev-seed.csv
poe db-reset     # run both commands above
poe db-down      # remove the local postgres container
```

## License

[MIT](LICENSE)
