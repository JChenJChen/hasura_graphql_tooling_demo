import yaml
import logging
import os

from hasura_tooling.util_filepath_and_fileloader import (
    hasura_metadata_tables,
    tables_metadata_filepath,
    sharded_tables_dir,
)

# the same as above but written differently
# from hasura_tooling import util_filepath_and_fileloader as file_utils
from hasura_tooling.update_permissions_e2e_test_mapping_metadata import (
    remove_permissions_e2e_test_mapping_metadata_by_table,
)
from hasura_tooling.shard_hasura_tables_yaml_lib import (
    refresh_tables_yaml_shards,
    reconstruct_sharded_hasura_tables_yaml,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def orchestrator(roles_input: str):
    roles = roles_input.lower().split("/")
    hasura_tables_metadata = hasura_metadata_tables()
    for table in hasura_tables_metadata:
        table_name = table["table"]["name"]
        try:
            table_select_permissions = table["select_permissions"]
            for permission in table_select_permissions:
                if permission["role"] in roles:
                    print("Removing permissions for {table}".format(table=table_name))
                    table["select_permissions"].remove(permission)
                    print(
                        "{role}'s {table}.".format(
                            role=permission["role"], table=table_name
                        )
                    )
        except Exception:
            print(
                "No select permissions metadata block for {table}".format(
                    table=table_name
                )
            )
            continue
    with open(tables_metadata_filepath(), "w") as f:
        yaml.dump(
            hasura_tables_metadata,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )
    remove_permissions_e2e_test_mapping_metadata_by_table(roles, [])


def delete_permission_shards(roles_input: str):
    logging.getLogger().setLevel(os.environ.get("LOGLEVEL", "INFO"))
    roles = roles_input.lower().split("/")
    refresh_tables_yaml_shards()
    sharded_tables_dir = sharded_tables_dir()
    for table_subdir in os.listdir(sharded_tables_dir):
        for shard in os.listdir(os.path.join(sharded_tables_dir, table_subdir)):
            if shard.replace(".yaml", "") in roles:
                shard_filepath = os.path.join(sharded_tables_dir, table_subdir, shard)
                if os.path.exists(shard_filepath):
                    os.remove(shard_filepath)
                else:
                    # should never end up here
                    logging.info(
                        f"SHARD DELETION FAILED, file not found: {shard_filepath}"
                    )
    reconstruct_sharded_hasura_tables_yaml(overwrite=True)
    remove_permissions_e2e_test_mapping_metadata_by_table(roles, [])
