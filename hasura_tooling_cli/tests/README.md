# Hasura/Graphql Tooling Pytests

## Setup

- navigate to root working directory: `cd` to `bin/hasura_tooling_cli`
- install python dependencies: `poetry install`
- create virtual environment: `poetry shell`

Additional setup steps for integration tests:

- review environmental variables: `cat envs`
  - PGPORT should be updated to whichever SDM-established port # of the postgres
    instance you're trying to test.
- setup environmental variables: `source envs`


## test_h_metadata_consistency_predeploy.py

**Overview:** Prevents Hasura metadata inconsistencies, with consideration for
postgres DB schema established by migrations in `servers/graphql2/migrations/`.

**How to run:**

- by file_name: `pytest tests/test_h_metadata_consistency_predeploy.py`

### test_all_tables_and_columns_in_metadata_exist_in_db_schema()

**Purpose:** Integration test that flags any tables and columns that are present
in `tables.yaml` but absent in postgres schema

**How to run:**

- by test_name regex: `pytest -k test_duplicate_role_permission_by_tables`

### test_duplicate_role_permission_by_tables()

Unit test that flags duplicate role select permissions by table, a common cause
of metadata inconsistency.

## test_api_postgres_tables_not_empty_or_missing.py

**Overview:** Integration test that checks that all postgres views/tables
available in API and have associated permissions are present and populated (with
at least 9 rows, bc that's the lowest valid table row count)

**How to run:**

- by test_name regex: `pytest -k test_api_pg_tables`
- by file_name: `pytest tests/test_api_postgres_tables_not_empty_or_missing.py`

## test_h_migrations.py

**Overview:** Unit tests that verify for Hasura migrations.

### test_no_h_migrations_precede_unit()

Unit test that checks no migrations precede the init migration. Hasura requires
and assumes that the init migration is first in the migrations list in order to
set up postgres's database schema before applying incremental update migrations
on top of the established schema.

### test_only_one_init_migration_exists()

Unit test that there only exists one init migration. There can only be one init
migration that establishes the postgres database schema.
