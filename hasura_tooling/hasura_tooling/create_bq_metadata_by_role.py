import os
from typing import Any, Dict, List
import yaml

from hasura_tooling.util_filepath_and_fileloader import (
    sharded_tables_dir,
    bigquery_api_metadata_folder,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def load_yaml_files(role: str, metadata_folder: str) -> dict:
    """
    Loads all yaml files in the sharded tables.yaml hasura metadata directory for a given role.
    """
    all_subdirs = []
    all_tables: Dict[str, Any] = dict()
    for subdir, _, files in os.walk(metadata_folder):
        if f"{role}.yaml" in files:
            all_subdirs.append(subdir)

    for table_folder in all_subdirs:
        table_name = str(os.path.basename(table_folder))
        all_tables[table_name] = dict()

        table_yaml_header = os.path.join(table_folder, "_table.yaml")
        with open(table_yaml_header, "r") as table_yaml_header_file:
            _table_yaml_content = yaml.safe_load(table_yaml_header_file)

        role_yaml_filename = os.path.join(table_folder, role + ".yaml")
        with open(role_yaml_filename, "r") as role_yaml_file:
            role_yaml_content = yaml.safe_load(role_yaml_file)
        all_tables[table_name] = {**_table_yaml_content, **role_yaml_content}

    return all_tables


def build_relationships(
    table_name: str, loaded_yaml_files: Dict[str, Any], relationship_type: str
) -> List[Any]:
    """Builds relationships BQ hasura metadata for a given table from PG API hasura metadata shards.

    Args:
        table_name (str)
        loaded_yaml_files (Dict[str, Any]): dictionary with all tables for given role
        relationship_type (str): array_relationships/object_relationships

    Returns:
        List[Any]: List of relationships for a given table
    """
    relationships = []
    if relationship_type in loaded_yaml_files[table_name].keys():
        for relationship in loaded_yaml_files[table_name][relationship_type]:
            remote_table = relationship["using"]["manual_configuration"][
                "remote_table"
            ]["name"]
            if remote_table in loaded_yaml_files.keys():
                relationship["using"]["manual_configuration"]["remote_table"].pop(
                    "schema"
                )
                relationship["using"]["manual_configuration"]["remote_table"][
                    "dataset"
                ] = "display_#timestamp#"
                relationship["using"]["manual_configuration"]["remote_table"][
                    "name"
                ] = remote_table
                relationships.append(relationship)
    return sorted(relationships, key=lambda x: x["name"])


def get_custom_table_name(table_name: str, table_yaml_file: Dict[str, Any]) -> str:
    """Returns the custom table name from PG API hasura metadata shards.

    Args:
        table_name (str): table name
        table_yaml_file (Dict[str, Any]): table yaml file

    Returns:
        str: custom table name
    """
    custom_name = table_name
    configuration = table_yaml_file.get("configuration")
    if configuration:
        custom_root_fields = configuration.get("custom_root_fields")
        if custom_root_fields:
            custom_name = custom_root_fields["select"]

    return f"{custom_name}"


def get_custom_column_name(table_yaml_file: Dict[str, Any]) -> dict:
    """Returns the custom column names from PG API hasura metadata shards.

    Args:
        table_name (str): table name
        table_yaml_file (Dict[str, Any]): table yaml file

    Returns:
        dict: custom column names
    """
    custom_column_names = {}
    configuration = table_yaml_file.get("configuration")
    if configuration:
        custom_column_names = configuration.get("custom_column_names", {})

    return custom_column_names


def build_table_info(table_name: str, table_yaml_file: Dict[str, Any]) -> dict:
    """Builds the table header hasura metadata for a given table.

    Args:
        table_name (str)

    Returns:
        dict: basic table info
    """
    _table = "table"
    _configuration = "configuration"

    table_info: Dict[str, Any] = dict()

    table_info[_table] = dict()
    table_info[_table]["dataset"] = "display_#timestamp#"
    table_info[_table]["name"] = table_name

    table_info[_configuration] = dict()
    table_info[_configuration]["custom_column_names"] = get_custom_column_name(
        table_yaml_file
    )
    table_info[_configuration]["custom_name"] = get_custom_table_name(
        table_name, table_yaml_file
    )
    table_info[_configuration]["custom_root_fields"] = dict()

    return table_info


def build_permissions(role: str, table_yaml_file: Dict[str, Any]) -> List[Any]:
    """Fills in role select permissions definition with BQ hasura API default values,
    and column permissions from corresponding PG API hasura metadata shards.
    """
    resultset_limit = 1000000
    permission = dict()
    permission["columns"] = table_yaml_file["permission"]["columns"]
    permission["filter"] = {}
    permission["limit"] = resultset_limit

    permission["allow_aggregations"] = True
    return [{"permission": permission, "role": role}]


def create_bq_api_metadata_by_role(role: str, bq_project: str, overwrite: bool):
    """Generates the BigQuery API metadata for a given role from its corresponding PG API metadata.

    Args:
        role (str): role to generate metadata for
        bq_project (str): BigQuery project to save metadata to
        overwrite (bool): whether to overwrite existing metadata
    """
    _object_relationships = "object_relationships"
    _array_relationships = "array_relationships"

    loaded_yaml_files = load_yaml_files(role, sharded_tables_dir())
    tables_list = []

    for table_name, table_yaml_file in loaded_yaml_files.items():
        tables_list.append(f"!include {role}_{table_name}.yaml")

        bq_api_yaml_content = build_table_info(table_name, table_yaml_file)

        # object relationships
        object_relationships = build_relationships(
            table_name, loaded_yaml_files, _object_relationships
        )
        if object_relationships:
            bq_api_yaml_content[_object_relationships] = object_relationships

        # array relationships
        array_relationships = build_relationships(
            table_name, loaded_yaml_files, _array_relationships
        )
        if array_relationships:
            bq_api_yaml_content[_array_relationships] = array_relationships

        bq_api_yaml_content["select_permissions"] = build_permissions(
            role, table_yaml_file
        )

        filename = os.path.join(
            bigquery_api_metadata_folder(bq_project), f"{role}_{table_name}.yaml"
        )
        with open(filename, "w+") as yaml_file:
            yaml.dump(
                bq_api_yaml_content,
                yaml_file,
                Dumper=IndentedListYamlDumper,
                default_flow_style=False,
                sort_keys=False,
            )

    tables_list.sort()

    filename = os.path.join(bigquery_api_metadata_folder(bq_project), "tables.yaml")
    with open(filename, "w+") as yaml_file:
        yaml.dump(
            tables_list,
            yaml_file,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=True,
        )
