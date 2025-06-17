import logging
import os
from typing import List, Dict

from hasura_tooling.util_filepath_and_fileloader import (
    yield_by_table_metadata,
)
from hasura_tooling.util_postgres_query import (
    concatenate_tuples_dict_list_to_single_dict_row,
    run_postgres_query,
)


def check_for_duplicate_role_permission_by_tables(
    hasura_tables_metadata: List, table_names: List
) -> Dict[str, list]:
    """checks for duplicate role permission declarations in Hasura metadata tables.yaml.

    Args:
        hasura_tables_metadata (list): Hasura metadata tables.yaml, current implementation is to have invoking function
            load tables.yaml into memory and passing it into this check function.
        table_names (list): list of tables to check for duplicate permission declarations. Current implementation is
            to have invoking function take in and parse slash-delimited tables list, and pass to this check function.

    Returns:
        dict: dictionary with description of duplicate declarations.
            dupe_perm_dict[table_name]=[roles_with_duplicate_permission_declarations]
    """
    dupe_perm_dict = {}
    for table in hasura_tables_metadata:
        table_name = table["table"]["name"]
        print("======= currently on table {}".format(table_name))
        if table_name in table_names:
            print("Checking: {table}".format(table=table_name))
            roles_list = []
            roles_with_duplicated_permissions = []
            for permission in table["select_permissions"]:
                if permission["role"] not in roles_list:
                    roles_list.append(permission["role"])
                else:
                    roles_with_duplicated_permissions.append(permission["role"])
            if len(roles_with_duplicated_permissions) > 0:
                dupe_perm_dict[table_name] = roles_with_duplicated_permissions
    try:
        assert len(dupe_perm_dict) == 0, "Roles with duplicate permissions:" + str(
            dupe_perm_dict
        )
        print("PASS: No duplicate permissions detected.")
    except AssertionError:
        raise
    return dupe_perm_dict


def test_header_shard_for_duplicate_keys(table_yaml_shard_filepath: str):
    """
    checks _table.yaml header shard for duplicate `select_permissions` keys prior to reconstructing tables.yaml.
    """
    try:
        with open(table_yaml_shard_filepath, "r") as f:
            table_yaml_shard = f.read()
    except FileNotFoundError as fnf_err:
        logging.exception(
            f"_table.yaml header shard not found: {table_yaml_shard_filepath}"
        )
        raise fnf_err
    try:
        # assertion for 'table' is >= 1 bc there should at least be the `table:` key, and if relationships exist,
        # then there will be relationship metadata keys including substring `table`
        assert table_yaml_shard.count("table") >= 1
        # if relationships/permissions exist, there should only exist at most 1 key by that name.
        assert table_yaml_shard.count("object_relationships") <= 1
        assert table_yaml_shard.count("array_relationships") <= 1
        assert table_yaml_shard.count("select_permissions") <= 1
    except AssertionError as err:
        logging.exception(
            f"Shard failed top-level keys assertion test: {table_yaml_shard_filepath}"
        )
        raise err


def collect_tables_and_columns_from_tables_yaml() -> dict:
    logging.info(
        "Collecting all tables & columns from graphql2/metadata/tables.yaml... please wait a min or three."
    )
    all_cols_and_tables_in_metadata: Dict[str, set] = {}
    for table in yield_by_table_metadata():
        table_name = table["table"]["name"]
        all_cols_and_tables_in_metadata[table_name] = set()
        if "select_permissions" in table.keys():
            for role in table["select_permissions"]:
                all_cols_and_tables_in_metadata[
                    table_name
                ] = all_cols_and_tables_in_metadata[table_name].union(
                    set(role["permission"]["columns"])
                )
        else:
            logging.info(f"{table_name} has no select permissions -- skipping.")
    logging.info("Tables & columns collection complete.")
    return all_cols_and_tables_in_metadata


def get_postgres_columns_by_table(table_name: str) -> List:
    return concatenate_tuples_dict_list_to_single_dict_row(
        run_postgres_query(
            f"""
        select column_name
        from information_schema.columns
        where table_schema = 'public'
        and table_name = '{table_name}';
        """
        )
    )["column_name"]


def check_if_postgres_table_exists(table_name: str) -> bool:
    res = run_postgres_query(
        f"""
        select table_name
        from information_schema.tables
        where table_schema = 'public'
        and table_name = '{table_name}';
        """
    )
    if len(res) == 0:
        return False
    elif len(res) == 1:
        return True
    else:
        # impossible case
        raise ValueError(
            f"Somehow there exists multiple public.{table_name} tables..?!"
        )


def get_all_tables_and_columns_in_metadata_not_in_db_schema() -> dict:
    """
    PURPOSE: The most common cause of Hasura migrations job failure during deployment is metadata inconsistencies due
    to tables and/or columns that are present in /graphql2/metadata/tables.yaml but absent in postgres's DB schema. This
    checks for that case.

    SETUP:
    1. `source envs` and double-check that PGPORT is pointed to intended postgres instance.
    Necessary step because tool introspects postgres for table_name and column_names.

    In case of psycopg2.OperationalError: verify env vars for postgres DB connection are setup.
    """
    logging.getLogger().setLevel(os.environ.get("LOGLEVEL", "INFO"))
    missing_in_postgres_dict = {}
    all_cols_and_tables_in_metadata = collect_tables_and_columns_from_tables_yaml()
    for table_name, columns in all_cols_and_tables_in_metadata.items():
        logging.info(f"Currently checking ----- {table_name}")
        if not check_if_postgres_table_exists(table_name):
            missing_in_postgres_dict[table_name] = columns
            logging.warning(f"TABLE MISSING FROM POSTGRES: {table_name}")
        else:
            not_in_pg_temp = []
            pg_columns = get_postgres_columns_by_table(table_name)
            for metadata_column in columns:
                if metadata_column not in pg_columns:
                    not_in_pg_temp.append(metadata_column)
                    logging.warning(
                        f"COLUMN MISSING FROM POSTGRES: {table_name}.{metadata_column}"
                    )
            if len(not_in_pg_temp) > 0:
                missing_in_postgres_dict[table_name] = not_in_pg_temp
    if len(missing_in_postgres_dict) > 0:
        print(
            "##### FAIL: BELOW TABLES/COLUMNS PRESENT IN METADATA AND MISSING IN POSTGRES"
        )
        print(missing_in_postgres_dict)
    else:
        print(
            "##### PASS: All tables and columns in tables.yaml Hasura metadata exist in postgres's DB schema."
        )
    return missing_in_postgres_dict
