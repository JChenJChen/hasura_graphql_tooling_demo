# See /bin/hasura_tooling_cli/README.md for downstream consuming bins.

## Description of `hasura_tooling` module

CRUD and testing tools to work with hasura, primarily metadata but migrations as
well.

## Table of Contents

> please refer to the CLI helper text for each script's description
>  (`python <script_name> --help`)

- `__init__.py`: Locally imports `hasura_tooling`, primarily for pytesting
- `append_allow_agg_and_computed_fields_to_shards.py`: one-off tooling that 
appends select API fields to sharded API metadata files.
- `check_hasura_metadata_tables_yaml.py`: hasura metadata checks
- `compare_hasura_permissions_definitions_lib.py`: compares hasura permissions
  definitions
- `create_or_append_relationship_e2e_tests.py`: creates new test scenarios or
  appends to existing karate E2E test feature files
- `get_empty_or_missing_api_tables_lib.py`: checks for missing or empty PG tables
  that are exposed in the graphql API
- `hasura_metadata_integrity_checker.py`: scans for and deletes duplicate
  permissions metadata.
- `hasura_metadata_sdk.py`: Hasura's SDK.
- `lookup_alias_by_actual_table_name.py`: translates aliased tables from their
  alias/production name to actual/native name
- `relationship_e2e_query_add_notnull.py`: one-off tooling
- `shard_hasura_tables_yaml_lib.py`: shards and reconstructs Hasura's aggregated API metadata
- `update_fn_create_generic_permissions_by_data_supersets.py`: main hasura
  tooling functionality script, creates new role with permissions.  
- `update_fn_create_relationships.py`: main hasura tooling functionality script,
creates new relationships.
- `update_fn_delete_all_permissions_by_roles.py`: main hasura tooling
  functionality script, deletes a role and its permissions metadata.
- `update_permissions_e2e_test_mapping_metadata.py`: updates role-to-test
  mappings in `hasura_metadata_perm.yaml`
- `util_filepath_and_fileloader.py`: file path and loading helper functions
- `util_introspection.py`: hasura metadata postgres database helper functions
- `util_postgres_query.py`: postgres database query functionality helper
  functions
- `util_yaml_dumper.py`: yaml indentation helper functions to adhere to 
  standards
- `remote_schema_permissions.py`: Add and remove remote schema permissions. 
They are treated separately from the rest of the metadata.
