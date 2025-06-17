from typing import List, Dict
import logging

from hasura_tooling.util_filepath_and_fileloader import (
    api_data_supersets_metadata,
    roles_metadata,
)
from hasura_tooling.util_postgres_query import create_postgres_connection
from hasura_tooling.lookup_alias_by_actual_table_name import (
    translate_actual_table_name_to_alias,
)
from hasura_tooling.util_postgres_query import run_postgres_query


def check_api_data_superset_keys_exist(api_data_supersets: list):
    api_data_superset_keys = list(api_data_supersets_metadata().keys())
    all_input_keys_in_metadata_keys = all(
        elem in api_data_superset_keys for elem in api_data_supersets
    )
    if not all_input_keys_in_metadata_keys:
        not_present_supersets = list(
            set(api_data_supersets) - set(api_data_superset_keys)
        )
        raise KeyError(
            f"The following input supersets do not exist in metadata_api_data_supersets.yaml:\
            {not_present_supersets}"
        )
    else:
        return all_input_keys_in_metadata_keys


def is_role_active(role: str) -> bool:
    try:
        return roles_metadata()[role]["is_active"]
    except KeyError:
        raise KeyError(f"Skipping {role} because not found in metadata_roles.yaml.")


def introspect_role_supersets(role: str) -> str:
    input_delimiter = "/"
    return input_delimiter.join(introspect_role_supersets_list(role))


def introspect_role_supersets_list(role: str) -> List[str]:
    return roles_metadata()[role]["api_data_supersets"]


def get_role_tables(role: str) -> list:
    role_tables = concatenate_tuples_dict_list_to_single_dict_row(
        run_postgres_query(
            """
        SELECT DISTINCT
        table_name
        FROM
        hdb_catalog.hdb_permission
        WHERE
        role_name = '{role}'
        ORDER BY
        table_name;""".format(
                role=role
            )
        )
    )["table_name"]
    # print(role_tables)
    return role_tables


def get_all_active_roles() -> list:
    get_all_active_roles = concatenate_tuples_dict_list_to_single_dict_row(
        run_postgres_query(
            """
        SELECT DISTINCT
        role_name
        FROM
        hdb_catalog.hdb_permission
        ORDER BY
        role_name;"""
        )
    )["role_name"]
    # print(get_all_active_roles)
    return get_all_active_roles


def concatenate_tuples_dict_list_to_single_dict_row(
    tuples_dict_list: list,
) -> Dict[str, list]:
    return_dict: Dict[str, list] = {}
    key = list(tuples_dict_list[0].keys())[0]
    return_dict[key] = []
    for row in tuples_dict_list:
        return_dict[key].append(row[key])
    return return_dict


def role_column_permissions_by_table_lookup(lookup_role: str, table_name: str) -> list:
    lookup_role = get_role_to_shadow_by_table(table_name)
    conn = create_postgres_connection()
    cur = conn.cursor()
    column_permissions_lookup_query = """
        SELECT
            perm_def -> 'columns'
        FROM
            hdb_catalog.hdb_permission
        WHERE
            perm_type = 'select' AND
            role_name = '{lookup_role}' AND
            table_name = '{table_name}';
        """.format(
        table_name=table_name, lookup_role=lookup_role
    )
    print(column_permissions_lookup_query)
    cur.execute(column_permissions_lookup_query)
    column_permissions_list = cur.fetchone()[0]
    return column_permissions_list


# TODO for now: make sure this function covers all datasupersets
# TODO ideal: this logic smells x.x should be dynamically determined
def get_role_to_shadow_by_table(table_name: str) -> str:
    obj_list = api_data_supersets_metadata()
    prd_table_name = translate_actual_table_name_to_alias(table_name)
    if prd_table_name in list(obj_list["shadow_role"].keys()):
        shadow_role = "shadow_role"
    else:
        shadow_role = "test_role"
    return shadow_role


def table_permissions_by_role_lookup(lookup_role: str) -> list:
    conn = create_postgres_connection()
    cur = conn.cursor()
    table_permissions_lookup_query = """
        SELECT
            table_name
        FROM
            hdb_catalog.hdb_permission
        WHERE
            role_name = '{lookup_role}';
        """.format(
        lookup_role=lookup_role
    )
    cur.execute(table_permissions_lookup_query)
    results = cur.fetchall()
    table_permissions_list = []
    for row in results:
        table_permissions_list.append(row[0])
    return table_permissions_list


def verify_table_names_list_input(tables: list) -> bool:
    for table_name in tables:
        res = run_postgres_query(
            """
            SELECT *
            FROM
            information_schema.tables
            WHERE
            table_name = '{table_name}' AND
            table_schema = 'public';
        """.format(
                table_name=table_name
            )
        )
        if len(res) != 1:
            raise ValueError(
                '### ERROR #### public.{table_name} does not exist in postgres, \
                or your database user currently lacks read permissions -- \
                Run "select grant_or_update_permissions(); in postgres."'.format(
                    table_name=table_name
                )
            )
            return False
    return True


def verify_role_names_list_input(roles: list) -> bool:
    for role in roles:
        res = run_postgres_query(
            """
            SELECT distinct role_name
            FROM
            hdb_catalog.hdb_permission
            WHERE
            role_name = '{role_name}';
        """.format(
                role_name=role
            )
        )
        if len(res) != 1:
            raise KeyError(
                '### ERROR #### {role_name} has no existing permissions in hdb_catalog.hdb_permission, \
                Are you sure the role_name is correct?"'.format(
                    role_name=role
                )
            )
            return False
    return True


def verify_column_names_list_input(table: str, columns: list) -> bool:
    for column in columns:
        res = run_postgres_query(
            """
            SELECT *
            FROM
            information_schema.columns
            WHERE
            table_schema = 'public' AND
            table_name = '{table}' AND
            column_name = '{column}';
            """.format(
                table=table, column=column
            )
        )
        if len(res) != 1:
            raise KeyError(
                '### ERROR #### {table} has no column named {column}\
                Are you sure the provided table and column names are correct?"'.format(
                    table=table, column=column
                )
            )
            return False
    return True


def transpose_to_data_superset_to_roles() -> Dict[str, list]:
    roles_dict = roles_metadata()
    superset_to_roles_dict: Dict[str, list] = {}
    for role, role_def in roles_dict.items():
        supersets = role_def["api_data_supersets"]
        for superset in supersets:
            if superset not in list(superset_to_roles_dict.keys()):
                superset_to_roles_dict[superset] = [role]
            else:
                superset_to_roles_dict[superset].append(role)
    return superset_to_roles_dict


def transpose_data_superset_combinations_to_roles() -> Dict[str, list]:
    roles_dict = roles_metadata()
    superset_combos_to_roles_dict: Dict[str, list] = {}
    for role, role_def in roles_dict.items():
        supersets = role_def["api_data_supersets"]
        if str(supersets) not in superset_combos_to_roles_dict.keys():
            superset_combos_to_roles_dict[str(supersets)] = [role]
        else:
            superset_combos_to_roles_dict[str(supersets)].append(role)
    return superset_combos_to_roles_dict


def get_subdir_by_table_name(origin_table: str):
    if origin_table in [
        "origin"
    ]:
        return "subdir"
    else:
        logging.error(f"Unknown subdir for: {origin_table}")


if __name__ == "__main__":
    pass
