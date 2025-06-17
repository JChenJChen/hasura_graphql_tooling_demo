import os
import yaml
import copy
import shutil
import logging

from hasura_tooling.util_filepath_and_fileloader import (
    sharded_tables_dir,
    tables_metadata_dir,
    yield_by_table_metadata,
)
from hasura_tooling.check_hasura_metadata_tables_yaml import (
    test_header_shard_for_duplicate_keys,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def truncate_sharded_tables_dir():
    """
    To prevent any extraneous outdated shards from being reconstructed into tables.yaml.
    Extraneous shards may exist if the shard was not deleted in metadata/tables/[table_name]/,
    and instead its contents were removed only in tables.yaml.
    """
    sharded_tables_dir = sharded_tables_dir()
    shutil.rmtree(sharded_tables_dir)
    os.mkdir(sharded_tables_dir)
    logging.info(
        f"Trunated and recreated {sharded_tables_dir} to freshly re-shard hasura metadata"
    )


def shard_hasura_tables_yaml():
    logging.info("Sharding /metadata/databases/default/tables metadata to shards...")
    for table_metadata_contents in yield_by_table_metadata():
        table_name = table_metadata_contents["table"]["name"]
        sharded_tables_subdir = os.path.join(sharded_tables_dir(), table_name)
        if not os.path.exists(sharded_tables_subdir):
            os.makedirs(sharded_tables_subdir)
        table_yaml_header_filename = os.path.join(sharded_tables_subdir, "_table.yaml")
        table_yaml_header_contents = copy.deepcopy(table_metadata_contents)
        try:
            table_yaml_role_perm_def_contents = copy.deepcopy(
                table_metadata_contents["select_permissions"]
            )
            for role_perm_def in table_yaml_role_perm_def_contents:
                role_name = role_perm_def["role"]
                role_perm_def["permission"]["columns"].sort()
                table_yaml_role_perm_def_filename = (
                    os.path.join(sharded_tables_subdir, role_name) + ".yaml"
                )
                with open(
                    table_yaml_role_perm_def_filename, "w"
                ) as table_yaml_role_perm_def_file:
                    yaml.dump(
                        role_perm_def,
                        table_yaml_role_perm_def_file,
                        Dumper=IndentedListYamlDumper,
                        default_flow_style=False,
                        sort_keys=False,
                    )
            # remove select permissions content from header file since each role's select permission will live in own file
            # set as empty to be repopulated later during reconstruction
            table_yaml_header_contents["select_permissions"] = []
        except Exception as e:
            logging.info(
                "{e}: No select permissions for {t}.".format(e=e, t=table_name)
            )
        with open(table_yaml_header_filename, "w") as table_yaml_header_file:
            yaml.dump(
                table_yaml_header_contents,
                table_yaml_header_file,
                Dumper=IndentedListYamlDumper,
                default_flow_style=False,
                sort_keys=False,
            )
    logging.info(
        f"{table_name}.yaml shards dumped to `servers/graphql2/metadata/tables/"
    )


def reconstruct_sharded_hasura_tables_yaml(overwrite: bool):
    sharded_tables_yaml_rootdir = sharded_tables_dir()
    table_yaml_contents = {}
    all_subdirs = []
    for subdir, dirs, files in os.walk(sharded_tables_yaml_rootdir):
        all_subdirs.append(subdir)
    all_subdirs.sort()
    # remove /metadata/tables/ dir bc it's not a table_subdir
    all_subdirs.remove(sharded_tables_dir())
    logging.info(
        "Dumping reconstructed metadata to /metadata/databases/default/tables..."
    )
    for table_subdir in all_subdirs:
        for subdir, dirs, files in os.walk(table_subdir):
            # if directory is table subdir (ex: metadata/tables/acris_legal)
            if dirs == []:
                table_yaml_header = os.path.join(subdir, "_table.yaml")
                # run table_yaml_header shard assertion tests prior to metadata reconstruction
                test_header_shard_for_duplicate_keys(table_yaml_header)
                with open(table_yaml_header, "r") as p:
                    table_yaml_table_contents = yaml.safe_load(p)
                files.sort(key=lambda f: f.replace(".yaml", ""))
                for file_name in files:
                    if file_name != "_table.yaml" and file_name.endswith(".yaml"):
                        with open(os.path.join(subdir, file_name), "r") as p:
                            role_perm_def = yaml.safe_load(p)
                            role_perm_def["permission"]["columns"].sort()
                        table_yaml_table_contents["select_permissions"].append(
                            role_perm_def
                        )
                table_yaml_contents = table_yaml_table_contents
            table_name = subdir.split("/")[subdir.split("/").index("tables") + 1]
            tables_yaml_filepath = os.path.join(
                tables_metadata_dir(), f"public_{table_name}.yaml"
            )
            with open(tables_yaml_filepath, "w") as table_yaml_file:
                yaml.dump(
                    table_yaml_contents,
                    table_yaml_file,
                    Dumper=IndentedListYamlDumper,
                    default_flow_style=False,
                    sort_keys=False,
                )
            table_yaml_file.close()
    logging.info("\n Tables metadata reconstruction complete.")


def refresh_tables_yaml_shards(refresh: bool = True):
    if refresh:
        truncate_sharded_tables_dir()
    shard_hasura_tables_yaml()
