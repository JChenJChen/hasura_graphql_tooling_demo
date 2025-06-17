# See /lib/hasura_tooling/README.md for info on consumed libs.

# Hasura Tooling

## Description of `hasura_tooling` module

CRUD and testing tools to work with hasura, primarily metadata but migrations as
well.

## /bin/hasura_tooling_cli/ Table of Contents

- `__init__.py`: Import /lib/hasura_tooling/.
- `__main__.py`: Entrypoint of main hasura tooling.
- `compare_hasura_permissions_definitions_cli.py`: compares hasura permissions
  definitions.
- `envs`: establish env vars
- `get_empty_or_missing_api_tables_cli.py`: checks for empty or missing API objects exposed in API
- `get_relationships_metadata.py`:
- `pyproject.toml`:
- `shard_hasura_tables_yaml_cli.py`: helper function that shards Hasura API metadata into shards 
that are organized within a directory structure
- `tests`
- `update_allow_agg_by_role.py`: toggles on/off allow_aggregation API query functionality by role.

### Setup

#### Poetry

1. `cd` to Hasura tooling cli bin directory -- `/bin/hasura_tooling_cli/`
2. `poetry install` to install Python packages listed in `pyproject.toml` with
   Poetry.
3. `poetry shell` to activate poetry's virtual environment with the installed
   dependencies.

#### Environment variables

- The database connection is typically created from
  `data/bin/hasura_tooling_cli/envs`, contents are provided below.
- Update the PGPORT to connect to the intended postgres instance.
- Run `source envs` to establish these environment variables in your terminal.

```bash
PGHOST=localhost
PGPORT=<PORT>
PGUSER=
PGPASSWORD=
PGDATABASE=<PGDATABASE>
```

### Quick-Start Guide/Workflow Steps

1. Setup poetry virtual environment with Python dependencies installed
   1. `cd` to graphql tooling directory -- `/bin/hasura_tooling_cli/`
   2. `poetry shell` to activate poetry's virtual environment with the installed
      dependencies.
2. Setup environment variables
   1. `cat envs` to check contents of envs file before exporting them as
      terminal environment variables. Usually the only variable that needs
      changing is `PGPORT` to specify which postgres database the tooling should
      be introspecting.
      1. if an environment variable needs to be updated, you can either:
         1. open `envs` up in IDE or terminal -> edit and save -> `source envs`
            to establish environment variables.
         2. `source envs` to establish environment variables, and then modify
            them for just the current terminal instance
   2. Double-check environment variables! Working off of wrong env vars is
      always a bad time. (ex: `env | grep PGPORT`)
3. Run Hasura metadata update tooling
   1. Walk thru the click help menu to find the needed functionality, and for
      syntax help.
      1. `python hasura_tooling_cli --help` will list all available
         functionalities.
   2. Step into the desired functionality to view the input args and example
      command syntax -- ex: `python hasura_tooling_cli sync-roles --help`
   3. replace `--help` with any necessary input args, if any.
5. Updating the E2E graphql test is taken care of for you because the Hasura
   metadata update tooling is integrated with the karate graphql test update
   tooling.

### Hasura Permissions Comparison Tooling

> re: compare_hasura_permissions_definitions_cli.py

Purpose: Hasura permissions investigations and analysis commonly require figuring out diffs to scope work.

See CLI `--help` helper text for usage details, example commands, and additional info.

Currently 3 functionalities:

1. Compare a role's actual permissions to its "prescription" as defined by source-of-truth metadata (roles & supersets).
2. Compare 2 supersets' permissions definitions against each other.
3. Compare 2 roles' actual permissions against each other.

Tooling Output:

- currently outputs a .txt file.
- With A and B as the first and second CLI cmd arguments respectively, output file sections include all sections of the diff "venn diagram":
  - only in A
  - only in B
  - in both A & B, but differ in definition.
  - If no diff, section header will say so.


### Components

1. `__main__.py` - Entry point that can be invoked with package name. Hosts the
   click CLI interface and helper menu and example command syntax, and calls
   workflow functionalities.
   1. Append `--help` to your command to pull up its corresponding helper menu.
      ex:
      1. `python hasura_tooling_cli --help`
      2. `python hasura_tooling_cli [functionality_name] --help`
2. Each workflow is facilitated by its own functionality .py file, which
   currently are:
   1. `sync-role-permissions-from-metadata` - sync permissions by role in
      accordance to the source-of-truth metadata yamls (`metadata_roles.yaml`,
      `metadata_api_data_supersets.yaml`)
   2. `create-relationships` - creates relationships from metadata. Current
      metadata implementation is `hasura_relationships_metadata.txt`, but future
      implementation is expected to use googlesheets as a human-interface, and
      have an intermediate step of source controlling the metadata prior to
      creating the relationships in the metadata.
   3. `create-bq-metadata` - Generates the BigQuery API metadata for a given
      role from its corresponding PG API metadata.
3. Helper functions/utilities
   1. `util_postgres_query.py`
      1. gets postgres connection string values from environment variables
      2. sets up postgres connection
      3. runs postgres queries
      4. organizes database results into Python objects
   2. `util_filepath_and_fileloader.py` - returns notable directory and
      filepaths for Hasura metadata+migrations, and e2e tests.
   3. `util_postgres_introspection.py` - introspects and verifies the state of
      postgres and Hasura, for an instance as specified by `envs`.
4. Coupled e2e tests update tooling
   1. `update_permissions_e2e_test_mapping_metadata.py` - updates permissions
      e2e test mapping metadata (`hasura_perm_metadata.yaml`) in correspondence
      to permissions updates made by tooling functionalities above.
   2. `create_or_append_relationship_e2e_tests.py` - will create or append
      relationship e2e test scenarios to feature files, and create graphql query
      files if one does not already exist.
