import os
import time
import yaml
from typing import Dict, List

from ruamel.yaml import YAML


def _get_repo_rootdir() -> str:
    # directory path is to be established by REPO_ROOTDIR environment variable in /hasura_tooling_cli/envs

    return str(os.environ.get("REPO_ROOTDIR"))


def graphql2_dir() -> str:
    default_dir = os.path.join(_get_repo_rootdir(), "servers", "graphql2")

    return os.environ.get("GRAPHQL2_ROOTDIR", default_dir)


def metadata_dir() -> str:
    return os.path.join(graphql2_dir(), "metadata")


def sharded_tables_dir() -> str:
    return os.path.join(metadata_dir(), "tables")


def permissions_definition_diff_dir() -> str:
    return os.path.join(os.getcwd(), "perm_def_diff")


def role_perm_vs_prescription_diff_dir() -> str:
    return os.path.join(permissions_definition_diff_dir(), "role_perm_vs_prescription")


def superset1_vs_superset2_diff_dir() -> str:
    return os.path.join(permissions_definition_diff_dir(), "superset_vs_superset")


def role1_perm_vs_role2_perm_diff_dir() -> str:
    return os.path.join(permissions_definition_diff_dir(), "role_vs_role")


def relationships_tooling_input_filepath() -> str:
    return os.path.join(metadata_dir(), "hasura_relationships_metadata.txt")


def hasura_metadata_dir() -> str:
    return os.path.join(graphql2_dir(), "metadata")


def tables_metadata_dir() -> str:
    return os.path.join(graphql2_dir(), "metadata", "databases", "default", "tables")


# CONFIG V3 CHANGE:
# - tables.yaml is now list of references to each table's metadata files ("!include <schema>_<table_name>.yaml")
# - and no longer contains the metadata itself
def tables_metadata_filepath() -> str:
    return os.path.join(tables_metadata_dir(), "tables.yaml")


def hasura_functions_dir() -> str:
    return os.path.join(graphql2_dir(), "metadata", "default", "functions")


def remote_schemas_filepath() -> str:
    return os.path.join(hasura_metadata_dir(), "remote_schemas.yaml")


# CONFIG V3 CHANGE:
# - tables.yaml is now list of references to each table's metadata files ("!include <schema>_<table_name>.yaml")
# - and no longer the metadata itself
def hasura_metadata_tables() -> List:
    with open(tables_metadata_filepath(), "r") as p:
        return yaml.safe_load(p)


def end_to_end_tests_root_dir() -> str:
    # also known as the permissions e2e test directory
    return os.path.join(graphql2_dir(), "tests", "feature", "features")


def relationships_end_to_end_dir() -> str:
    return os.path.join(end_to_end_tests_root_dir(), "relationships")


def address_end_to_end_dir() -> str:
    return os.path.join(end_to_end_tests_root_dir(), "address")


def permissions_e2e_tests_mapping_metadata_filepath() -> str:
    return os.path.join(end_to_end_tests_root_dir(), "hasura_perm_metadata.yaml")


def permissions_e2e_tests_mapping_metadata() -> Dict[str, dict]:
    with open(permissions_e2e_tests_mapping_metadata_filepath(), "r") as p:
        return yaml.safe_load(p)


def hasura_source_of_truth_metadata_dir() -> str:
    return os.path.join(metadata_dir(), "source_of_truth")


def supersets_metadata_filepath() -> str:
    return os.path.join(
        hasura_source_of_truth_metadata_dir(), "metadata_api_data_supersets.yaml"
    )


def roles_metadata_filepath() -> str:
    return os.path.join(hasura_source_of_truth_metadata_dir(), "metadata_roles.yaml")


def domain_rules_metadata_filepath() -> str:
    return os.path.join(
        hasura_source_of_truth_metadata_dir(), "domain_rules.yaml"
    )


def bigquery_api_metadata_folder(bq_project: str) -> str:
    return os.path.join(
        graphql2_dir(), bq_project, "metadata", "databases", bq_project, "tables"
    )


def api_data_supersets_metadata() -> Dict[str, dict]:
    with open(supersets_metadata_filepath(), "r") as y:
        return yaml.safe_load(y)


def roles_metadata() -> Dict[str, dict]:
    with open(roles_metadata_filepath(), "r") as y:
        return yaml.safe_load(y)


def remote_schemas_metadata() -> List[dict]:
    with open(remote_schemas_filepath(), "r") as y:
        return YAML().load(y)


def domain_rules_metadata() -> Dict[str, dict]:
    with open(domain_rules_metadata_filepath(), "r") as y:
        return yaml.safe_load(y)


def unix_timestamp_prefix() -> int:
    unix_timestamp_prefix = int(time.time() * 1000)
    return unix_timestamp_prefix


def migrations_dir() -> str:
    # returns the local folder path for graphql2/migrations/default/ repo directory
    migrations_dir = os.path.join(graphql2_dir(), "migrations", "default")
    return migrations_dir


def yield_by_table_metadata():
    for subdir, _, files in os.walk(tables_metadata_dir()):
        for hasura_metadata_table_file in files:
            if hasura_metadata_table_file != "tables.yaml":
                with open(os.path.join(subdir, hasura_metadata_table_file), "r") as p:
                    table_metadata_contents = yaml.safe_load(p)
                yield table_metadata_contents
