from typing import List, Dict, Set, Any

import yaml
import copy
import os
import logging

from hasura_tooling.util_filepath_and_fileloader import (
    api_data_supersets_metadata,
    sharded_tables_dir,
    hasura_metadata_tables,
    tables_metadata_filepath,
)
from hasura_tooling.update_permissions_e2e_test_mapping_metadata import (
    update_permissions_e2e_test_mapping_metadata_by_table,
)
from hasura_tooling.util_introspection import check_api_data_superset_keys_exist
from hasura_tooling.shard_hasura_tables_yaml_lib import (
    refresh_tables_yaml_shards,
    reconstruct_sharded_hasura_tables_yaml,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def get_api_tables_list_from_api_data_supersets(api_data_supersets: list) -> list:
    """Reads in the established api_data_supersets from the metadata yaml.
    """
    api_data_supersets_dict = api_data_supersets_metadata()
    consolidated_tables_update_list: List[str] = []
    for api_table_set in api_data_supersets:
        if api_table_set not in list(api_data_supersets_dict.keys()):
            raise ValueError("invalid object_set.")
        consolidated_tables_update_list += api_data_supersets_dict[api_table_set].keys()
    return list(set(consolidated_tables_update_list))


def permissions_block_template(
    role, columns, limit, allow_aggregations, computed_fields, filter
):
    t = {
        "role": role,
        "permission": {
            "columns": columns,
            "filter": filter,
            "limit": limit,
            "computed_fields": computed_fields,
            "allow_aggregations": allow_aggregations,
        },
    }
    return t


def merge_table_permission_definitions(table1: dict, table2: dict) -> dict:
    merged_dict = {}
    # get all defined permissions definition keys (ex: columns, limit, filter, etc.)
    all_perm_def_keys: Set[Any] = set().union(*[table1, table2])
    for perm_def_key in all_perm_def_keys:
        if perm_def_key == "limit":
            # at least one perm_def must have limit defined (no default value) for the result to be complete.
            if "limit" not in table1 and "limit" not in table2:
                raise AssertionError(
                    "no limit permissions definition for both table1 and table2"
                )
            merged_dict["limit"] = max(table1.get("limit", 0), table2.get("limit", 0))
        elif perm_def_key == "columns":
            # at least one perm_def must have columns defined for the result to be complete.
            if "columns" not in table1 and "columns" not in table2:
                raise AssertionError(
                    "no column permissions definition for both table1 and table2"
                )
            merged_dict["columns"] = list(
                set(table1.get("columns", []) + table2.get("columns", []))
            )
        # Allow_agg = True if either is. If not defined, defaults to False.
        elif perm_def_key == "allow_aggregations":
            merged_dict["allow_aggregations"] = table1.get(
                "allow_aggregations", False
            ) or table2.get("allow_aggregations", False)
        elif perm_def_key == "computed_fields":
            # take whichever computed_fields is present, or set to default value if neither present.
            if (
                "computed_fields" not in table1
                or "computed_fields" not in table2
                or table1["computed_fields"] is None
                or table2["computed_fields"] is None
            ):
                merged_dict["computed_fields"] = (
                    table1.get("computed_fields", None)
                    or table2.get("computed_fields", None)
                    or []
                )
            elif table1["computed_fields"] == [] or table2["computed_fields"] == []:
                merged_dict["computed_fields"] = (
                    table1["computed_fields"] + table2["computed_fields"]
                )
            elif table1["computed_fields"] == table2["computed_fields"]:
                # if both not-empty but same, take either.
                merged_dict["computed_fields"] = table1["computed_fields"]
            else:
                raise AssertionError(
                    "computed_fields in table1 and table2 different & not empty, can't auto-merge."
                )
        elif perm_def_key == "filter":
            # take whichever filter is present, or set to default value if neither present.
            if table1.get("filter", {}) == {} or table2.get("filter", {}) == {}:
                merged_dict["filter"] = (
                    table1.get("filter", None) or table2.get("filter", None) or {}
                )
            elif table1["filter"] == table2["filter"]:
                # if both not-empty but same, take either
                merged_dict["filter"] = table1["filter"]
            else:
                raise AssertionError(
                    "filter in table1 and table2 different & not empty, can't auto-merge."
                )
        else:
            raise AssertionError(
                f"Unexpected permissions_definition key: {perm_def_key}"
            )
    return merged_dict


def get_api_tables_combined_dictionary(api_data_supersets: list) -> dict:
    """Reads in the established api_data_supersets from the metadata yaml.
    """
    api_data_supersets_dict = api_data_supersets_metadata()
    consolidated_tables_update_dict: Dict[str, dict] = {}
    for superset_name, superset_perm_def in api_data_supersets_dict.items():
        if superset_name in api_data_supersets:
            for table_name, table_perm_def in superset_perm_def.items():
                table_perm_def_copy = copy.deepcopy(table_perm_def)
                if table_name not in list(consolidated_tables_update_dict.keys()):
                    # roles with only a single superset will go thru this logic path, which skips the merge function
                    # that handles deduping columns amongst other processing steps.
                    # line below dedupes columns in case of manual duplication in metadata_api_data_supersets.yaml
                    seen = set()
                    dupes = set()
                    for item in table_perm_def.get("columns", []):
                        if item in seen:
                            dupes.add(item)
                        else:
                            seen.add(item)

                    if dupes:
                        logging.warning(
                            f"Duplicate column(s) {list(dupes)} detected in {superset_name}.{table_name}.\n\
                                Resulting column permissions deduped, but please dedupe the perm_def in metadata_api_data_supersets.yaml"
                        )
                    consolidated_tables_update_dict[table_name] = table_perm_def_copy
                else:
                    consolidated_tables_update_dict[
                        table_name
                    ] = merge_table_permission_definitions(
                        consolidated_tables_update_dict[table_name], table_perm_def_copy
                    )
    return consolidated_tables_update_dict


def remove_extraneous_table_shards_not_in_superset_prescription(
    role: str, consolidated_tables_update_dict: dict
):
    """
    Removes any extraneous permissions definition shards for tables that are not in the role's
     prescribed supersets' list of tables.
    """
    prescribed_superset_tables_list = consolidated_tables_update_dict.keys()
    sharded_tables_yaml_rootdir = sharded_tables_dir()
    role_file_name = role + ".yaml"
    for subdir, dirs, files in os.walk(sharded_tables_yaml_rootdir):
        # skip `metadata/tables/` subdir bc it's not a table shards subdir
        if subdir == sharded_tables_dir():
            continue
        table_name = subdir.split("/")[subdir.split("/").index("tables") + 1]
        if (table_name not in prescribed_superset_tables_list) and (
            role_file_name in files
        ):
            # parsing logic: table_name always follow `metadata/tables`.
            # # This is safer than taking last element blindly.
            shard_filepath = os.path.join(subdir, role_file_name)
            try:
                os.remove(shard_filepath)
            except FileNotFoundError:
                # should never end up here
                raise FileNotFoundError(
                    f"SHARD DELETION FAILED, file not found: {shard_filepath}"
                )


def table_permissions_metadata_header(table_name: str):
    t = {"table": {"schema": "public", "name": table_name}, "select_permissions": []}
    return t


def include_new_table_in_table_yaml(table_name):
    include_tables_yaml = hasura_metadata_tables()
    include_tables_yaml.append(f"!include public_{table_name}.yaml")
    with open(tables_metadata_filepath(), "w") as f:
        yaml.dump(
            include_tables_yaml,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


def sync_permission_shards_by_roles(roles_input: str, api_data_supersets_input: str):
    # TODO: more robust way of taking in & parsing api_data_supersets input parameter.
    logging.getLogger().setLevel(os.environ.get("LOGLEVEL", "INFO"))
    roles = roles_input.lower().split("/")
    api_data_supersets = api_data_supersets_input.lower().split("/")
    if check_api_data_superset_keys_exist(api_data_supersets):
        consolidated_tables_update_dict = get_api_tables_combined_dictionary(
            api_data_supersets
        )
        refresh_tables_yaml_shards()
        for table_name in consolidated_tables_update_dict:
            sharded_table_subdir = os.path.join(sharded_tables_dir(), table_name)
            # case: new table added in metadata_api_data_supersets.yaml
            if not os.path.isdir(sharded_table_subdir):
                # create shard subdir for new table
                os.mkdir(sharded_table_subdir)
                # create stub _table.yaml
                with open(os.path.join(sharded_table_subdir, "_table.yaml"), "w") as f:
                    yaml.dump(
                        table_permissions_metadata_header(table_name),
                        f,
                        Dumper=IndentedListYamlDumper,
                        default_flow_style=False,
                        sort_keys=False,
                    )
                include_new_table_in_table_yaml(table_name)
            for role in roles:
                # remove any extraneous perm_def shards of tables not in role's supersets prescription table list
                remove_extraneous_table_shards_not_in_superset_prescription(
                    role, consolidated_tables_update_dict
                )
                sharded_role_perm_file = (
                    os.path.join(sharded_table_subdir, role) + ".yaml"
                )
                columns = consolidated_tables_update_dict[table_name].get("columns", [])
                columns.sort()
                # if allow_agg=False or computed_fields=[] aren't already
                allow_aggregations = consolidated_tables_update_dict[table_name].get(
                    "allow_aggregations", False
                )
                limit = consolidated_tables_update_dict[table_name].get("limit", 100)
                filter = consolidated_tables_update_dict[table_name].get("filter", {})
                computed_fields = consolidated_tables_update_dict[table_name].get(
                    "computed_fields", []
                )
                role_permission_block = permissions_block_template(
                    role, columns, limit, allow_aggregations, computed_fields, filter
                )
                with open(sharded_role_perm_file, "w") as f:
                    yaml.dump(
                        role_permission_block,
                        f,
                        Dumper=IndentedListYamlDumper,
                        default_flow_style=False,
                        sort_keys=False,
                    )
        reconstruct_sharded_hasura_tables_yaml(True)
        update_permissions_e2e_test_mapping_metadata_by_table(
            roles, list(consolidated_tables_update_dict.keys()), True
        )
