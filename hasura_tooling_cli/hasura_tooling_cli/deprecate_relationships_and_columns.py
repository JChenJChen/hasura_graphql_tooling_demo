import yaml
import logging
import os
import copy

from hasura_tooling.util_filepath_and_fileloader import (
    sharded_tables_dir,
    api_data_supersets_metadata,
    supersets_metadata_filepath,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def deprecated_columns_metadata() -> list:
    return [
        {"table_name": "table_1", "column_name": "column_1"},
        {"table_name": "table_1", "column_name": "column_2"},
        {"table_name": "table_2", "column_name": "column_3"},
    ]


def deprecated_relationships_metadata() -> list:
    return [
        {
            "origin_table": "origin_table_1",
            "remote_table": "remote_table_1",
            "rel_name": "origin_1__remote_1",
            "replacement_rel_name": "origin_1__remote_1_new",
        },
        {
            "origin_table": "origin_table_1",
            "remote_table": "remote_table_2",
            "rel_name": "origin_1__remote_2",
            "replacement_rel_name": "origin_1__remote_2_new",
        },
        {
            "origin_table": "origin_table_1",
            "remote_table": "remote_table_3",
            "rel_name": "origin_1__remote_3",
            "replacement_rel_name": "origin_1__remote_3_new",
        },
    ]


def deprecate_relationships():
    deprec_rels_metadata = deprecated_relationships_metadata()
    sharded_tables_dir = sharded_tables_dir()
    replacement_rels_created = 0
    replacement_rels_found_or_skipped = 0
    replacement_rels_list = []
    for row in deprec_rels_metadata:
        rel_found = False
        rel_replaced = False
        # print("\n")
        # print(row)
        table_name = row["origin_table"]
        table_yaml_header_file = os.path.join(
            sharded_tables_dir, table_name, "_table.yaml"
        )
        with open(table_yaml_header_file, "r") as p:
            table_yaml_contents = yaml.safe_load(p)
        table_yaml_contents_output = copy.deepcopy(table_yaml_contents)
        replacement_rel_metadata = []
        replacement_rel_type = ""
        if "object_relationships" in table_yaml_contents:
            for obj_rel in table_yaml_contents["object_relationships"]:
                if (
                    obj_rel["name"] == row["rel_name"]
                    and obj_rel["using"]["manual_configuration"]["remote_table"]["name"]
                    == row["remote_table"]
                    and not rel_replaced
                ):
                    logging.info(
                        f"matching relationship found: {row['origin_table']}.{row['rel_name']}"
                    )
                    replacement_rel_metadata = copy.deepcopy(obj_rel)
                    replacement_rel_type = "object"
                    # relationships are unique when grouped by origin_table & rel_name, so it's safe to assume
                    # if a relationship is found under obj_rels, it's the only rel to be updated for that metadata_row,
                    # and the array_relationship for-loop below can be skipped to prevent unintentional removal of the
                    # updated relationship
                    rel_found = True
                    for obj_rel2 in table_yaml_contents["object_relationships"]:
                        if (
                            obj_rel2["name"] == row["replacement_rel_name"]
                            and obj_rel2["using"]["manual_configuration"][
                                "remote_table"
                            ]["name"]
                            == row["remote_table"]
                        ) or (row["replacement_rel_name"] == "n/a"):
                            # if replacement relationship exists, delete to-be-deprecated relationship
                            table_yaml_contents_output["object_relationships"].remove(
                                obj_rel
                            )
                            logging.info(
                                f"replacement relationship found, removing {row['rel_name']}"
                            )
                            rel_replaced = True
                            replacement_rels_found_or_skipped += 1
                            break
        if not rel_found:
            if "array_relationships" in table_yaml_contents:
                for array_rel in table_yaml_contents["array_relationships"]:
                    if (
                        array_rel["name"] == row["rel_name"]
                        and array_rel["using"]["manual_configuration"]["remote_table"][
                            "name"
                        ]
                        == row["remote_table"]
                        and not rel_replaced
                    ):
                        logging.info(
                            f"matching relationship found: {row['origin_table']}.{row['rel_name']}"
                        )
                        replacement_rel_metadata = copy.deepcopy(array_rel)
                        replacement_rel_type = "array"
                        rel_found = True
                        for array_rel2 in table_yaml_contents["array_relationships"]:
                            if (
                                array_rel2["name"] == row["replacement_rel_name"]
                                and array_rel2["using"]["manual_configuration"][
                                    "remote_table"
                                ]["name"]
                                == row["remote_table"]
                            ) or (row["replacement_rel_name"] == "n/a"):
                                # if replacement relationship exists, delete to-be-deprecated relationship
                                table_yaml_contents_output[
                                    "array_relationships"
                                ].remove(array_rel)
                                logging.info(
                                    f"replacement relationship found, removing {row['rel_name']}"
                                )
                                rel_replaced = True
                                replacement_rels_found_or_skipped += 1
                                break
                if not rel_found:
                    logging.error(
                        f"to-be-deprecated relationship not found/is missing: {row}"
                    )
                    continue
        # only if relationship is found and not replaced yet
        if not rel_replaced:
            logging.info("No replacement relationship found...")
            replacement_rel_metadata["name"] = row["replacement_rel_name"]
            table_yaml_contents_output[f"{replacement_rel_type}_relationships"].append(
                replacement_rel_metadata
            )
            replacement_rels_created += 1
            replacement_rels_list.append(
                (row["origin_table"], row["remote_table"], row["replacement_rel_name"])
            )
        with open(table_yaml_header_file, "w") as w:
            yaml.dump(
                table_yaml_contents_output,
                w,
                Dumper=IndentedListYamlDumper,
                default_flow_style=False,
                sort_keys=False,
            )
    print(replacement_rels_created)
    print(replacement_rels_found_or_skipped)


def deprecate_columns():
    deprec_cols_metadata = deprecated_columns_metadata()
    sharded_tables_dir = sharded_tables_dir()
    for row in deprec_cols_metadata:
        table_name = row["table_name"]
        for subdir, dirs, files in os.walk(
            os.path.join(sharded_tables_dir, table_name)
        ):
            for role_shard_yaml in files:
                if role_shard_yaml != "_table.yaml":
                    role_shard_yaml_filepath = os.path.join(
                        sharded_tables_dir, table_name, role_shard_yaml
                    )
                    with open(role_shard_yaml_filepath, "r") as p:
                        table_yaml_contents = yaml.safe_load(p)
                    for column in table_yaml_contents["permission"]["columns"]:
                        if column == row["column_name"]:
                            table_yaml_contents["permission"]["columns"].remove(column)
                            with open(role_shard_yaml_filepath, "w") as role_shard_yaml:
                                yaml.dump(
                                    table_yaml_contents,
                                    role_shard_yaml,
                                    Dumper=IndentedListYamlDumper,
                                    default_flow_style=False,
                                    sort_keys=False,
                                )
                            logging.info(
                                f"matching column found & deprecating: {row['table_name']}.{row['column_name']}"
                            )


def deprecate_columns_in_supersets():
    deprec_cols_metadata = deprecated_columns_metadata()
    consolidated_deprec_dict = {}
    for row in deprec_cols_metadata:
        if row["table_name"] not in consolidated_deprec_dict:
            consolidated_deprec_dict[row["table_name"]] = [row["column_name"]]
        else:
            consolidated_deprec_dict[row["table_name"]].append(row["column_name"])
    supersets_metadata = api_data_supersets_metadata()
    supersets_metadata_output = copy.deepcopy(supersets_metadata)
    for superset, table_perm_def in supersets_metadata.items():
        if superset == "superset1":
            for table, perm_def in table_perm_def.items():
                if table in consolidated_deprec_dict:
                    if "columns" in perm_def.keys():
                        for column in perm_def["columns"]:
                            if column in consolidated_deprec_dict[table]:
                                supersets_metadata_output[superset][table][
                                    "columns"
                                ].remove(column)
    with open(supersets_metadata_filepath(), "w") as w:
        yaml.dump(
            supersets_metadata_output,
            w,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(os.environ.get("LOGLEVEL", "INFO"))
    deprecate_relationships()
    deprecate_columns()
    deprecate_columns_in_supersets()
