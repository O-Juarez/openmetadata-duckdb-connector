# OpenMetadata DuckDB Connector
This repository is an custom [OpenMetadata](https://open-metadata.org/)'s [DuckDB](https://duckdb.org/) Connector.
![openmetadata_duckdb.png](images%2Fopenmetadata_duckdb.png)

## Step 1 - Prepare the connector
A connector is a class that extends from `metadata.ingestion.api.source.Source`. It should implement
all the required methods ([docs](https://docs.open-metadata.org/sdk/python/build-connector/source#for-consumers-of-openmetadata-ingestion-to-define-custom-connectors-in-their-own-package-with-same-namespace)).

In `connector/duckdb_connector.py` you have a minimal example of it.

Note how te important method is the `next_record`. This is the generator function that will be iterated over
to send all the Create Entity Requests to the `Sink`. Read more about the `Workflow` [here](https://docs.open-metadata.org/sdk/python/build-connector).

## Step 2 - Yield data
The `Sink` is expecting Create Entity Requests. To get familiar with the Python SDK and understand how to create
the different Entities, a recommended read is the Python SDK [docs](https://docs.open-metadata.org/sdk/python).

We do not have docs and examples of all the supported Services. A way to get examples on how to create and fetch
other types of Entities is to directly refer to the `ometa` [integration tests](https://github.com/open-metadata/OpenMetadata/tree/main/ingestion/tests/integration/ometa).

## Step 3 - Prepare the package installation
We'll need to package the code so that it can be shipped to the ingestion container and used there. In this demo
you can find a simple `setup.py` that builds the `connector` module.

## Step 4 - Prepare the Ingestion Image

If you want to use the connector from the UI, the `openmetadata-ingestion` image should be aware of your new package.

We will be running the demo against the OpenMetadata version `1.1.2`, therefore, our Dockerfile looks like:

```Dockerfile
# Base image from the right version
FROM openmetadata/ingestion:1.1.2

# Let's use the same workdir as the ingestion image
WORKDIR ingestion
USER airflow

# Install our custom connector
# For a PROD image, this could be picking up the package from your private package index
COPY connector connector
COPY setup.py .
RUN pip install --no-deps .
```
Build and use the new openmetadata-ingestion images in Docker compose:
```yaml
  ingestion:
    container_name: openmetadata_ingestion
    build:
      context: ../
      dockerfile: docker/Dockerfile
```

## Step 5 - Run OpenMetadata with the custom Ingestion image

We have a `Makefile` prepared for you to run `make run`. This will get OpenMetadata up in Docker Compose using the
custom Ingestion image.

## Step 6 - Configure the Connector

In this guide we prepared a Database Connector. Thus, go to `Database Services > Add New Service > Custom`
and set the `Source Python Class Name` as `connector.duckdb_connector.DuckDBConnector`.

Note how we are specifying the full module name so that the Ingestion Framework can import the Source class.

---

## DuckDB Custom Connector

To run the DuckDB Custom Connector, the Python class will be `connector.duckdb_connector.DuckDBConnector` and we'll need
to set the following Connection Options:
- `database_name`: The name of DuckDB database
- `database_schema_list`: List database schema splits by comma, eg. `dimensions, facts, marts`
- `database_file_path`: Path to a local DuckDB file, or a MotherDuck URI like `md:my_db`

For MotherDuck, export the `MOTHERDUCK_TOKEN` env var before running ingestion — DuckDB reads it natively, so the token never lives in the YAML. The `CustomDatabaseConnection` schema has no encrypted-secret fields, so anything placed in `connectionOptions` would be stored in plaintext.

Each workflow ships as a `*.example.yml` template (placeholders, safe to commit) and a `*.yml` (your active, runnable config — typically gitignored when it has secrets):

| Template | Active config | Purpose |
|---|---|---|
| [duckdb_ingestion.example.yml](duckdb_ingestion.example.yml) | [duckdb_ingestion.yml](duckdb_ingestion.yml) | Custom-connector metadata ingestion (local file or MotherDuck) |
| [duckdb_metadata.example.yml](duckdb_metadata.example.yml) | [duckdb_metadata.yml](duckdb_metadata.yml) | Native OM Duckdb metadata ingestion (required for profiler) |
| [duckdb_profiler.example.yml](duckdb_profiler.example.yml) | [duckdb_profiler.yml](duckdb_profiler.yml) | External profiler workflow (sampling, stats) |
| [duckdb_dbt.example.yml](duckdb_dbt.example.yml) | [duckdb_dbt.yml](duckdb_dbt.yml) | DBT manifest → lineage edges |

Copy a template to its `.yml` form, fill in the placeholders, then run `metadata ingest -c <file>` (or `metadata profile -c …` for the profiler).

---

## Testing

The connector ships with a [pytest](https://pytest.org) suite covering type normalization, config validation, file-existence checks, the `_iter` order, MotherDuck detection and token plumbing, and the `create()` classmethod.

### With [uv](https://docs.astral.sh/uv/) (recommended)

```bash
cd duckdb-connector
uv sync                  # creates .venv with project + dev deps
uv run pytest -v
```

### With plain pip

```bash
cd duckdb-connector
python -m venv .venv && source .venv/bin/activate
pip install -e . pytest
pytest -v
```

### What the tests cover

- `tests/test_normalize_type.py` — pure-function mapping of DuckDB types to OpenMetadata types.
- `tests/test_duckdb_connector.py`:
  - `TestInit` — connection-option validation (missing `database_name`, `database_schema_list`, `database_file_path`; whitespace/empty entries stripped).
  - `TestPrepare` — raises on missing local file; succeeds when present; skips file check for `md:` URIs.
  - `TestTestConnection` — opens the file and runs `SELECT 1`.
  - `TestIter` — yields service → database → schemas → tables in order; missing schemas surface as `Either(left=...)` errors instead of crashing.
  - `TestCreate` — instantiates from a real `WorkflowSource` config dict.
  - `TestMotherDuck` — `md:` prefix detection, token passthrough to `duckdb.connect`, env-var fallback.

Tests using the OpenMetadata SDK auto-skip if `openmetadata-ingestion` isn't installed (`pytest.importorskip`), so the pure-function tests still run in a minimal environment.