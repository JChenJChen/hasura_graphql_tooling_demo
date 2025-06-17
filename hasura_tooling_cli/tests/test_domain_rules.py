import pytest
import logging
import yaml
import itertools
from typing import Dict

from hasura_tooling.hasura_metadata_sdk import (
    table_entry_from_dict,
    select_permission_from_dict,
    select_permission_to_dict,
)
from hasura_tooling.util_filepath_and_fileloader import (
    hasura_metadata_tables,
    roles_metadata,
    domain_rules_metadata,
)
from hasura_tooling.compare_hasura_permissions_definitions_lib import (
    diff_2_perm_def_dicts,
)


logging.basicConfig(level=logging.DEBUG)


@pytest.fixture(scope="session")
def tables_yaml_with_hasura_obj_model():
    tables_yaml_contents = hasura_metadata_tables()
    return [table_entry_from_dict(table_entry) for table_entry in tables_yaml_contents]


@pytest.fixture(scope="session")
def tables_and_roles_hash_dict(tables_yaml_with_hasura_obj_model: list):
    hash_dict: Dict[str, dict] = dict()
    for table_entry in tables_yaml_with_hasura_obj_model:
        table_name = table_entry.table.name
        hash_dict[table_name] = {
            "index": tables_yaml_with_hasura_obj_model.index(table_entry)
        }
        hash_dict[table_name]["roles"] = dict()
        if table_entry.select_permissions:
            for role in table_entry.select_permissions:
                role_name = role.role
                hash_dict[table_name]["roles"][
                    role_name
                ] = table_entry.select_permissions.index(role)
        else:
            logging.warning(
                f"No select_permissions in {table_name}. Skipping roles hash dict creation."
            )
    return hash_dict


# @pytest.fixture(scope='session')
def setup_ui_access_roles_lkp_table_permdef_requirements() -> list:
    rules = domain_rules_metadata()
    ui_roles = rules["test_ui_roles_lkp_tables_permdef"]["ui_roles"]
    lkp_tables = list(
        rules["test_ui_roles_lkp_tables_permdef"][
            "lkp_permdef_reqs_for_ui_roles"
        ].keys()
    )
    return [element for element in itertools.product(lkp_tables, ui_roles)]


@pytest.mark.parametrize(
    "lkp_table_name, ui_role", setup_ui_access_roles_lkp_table_permdef_requirements()
)
def test_ui_access_roles_lkp_table_permdef_requirements(
    lkp_table_name: str,
    ui_role: str,
    tables_yaml_with_hasura_obj_model: list,
    domain_rules: dict,
    tables_and_roles_hash_dict: dict,
):
    assert ui_role in tables_and_roles_hash_dict[lkp_table_name]["roles"]
    table_entry = tables_yaml_with_hasura_obj_model[
        tables_and_roles_hash_dict[lkp_table_name]["index"]
    ]
    roles_hash_dict = tables_and_roles_hash_dict[lkp_table_name]["roles"]
    role_permissions = table_entry.select_permissions[
        roles_hash_dict[ui_role]
    ].permission
    lkp_permdef_req = select_permission_from_dict(
        domain_rules["test_ui_roles_lkp_tables_permdef"][
            "lkp_permdef_reqs_for_ui_roles"
        ][lkp_table_name]
    )
    logging.info(
        diff_2_perm_def_dicts(
            {lkp_table_name: select_permission_to_dict(role_permissions)},
            {lkp_table_name: select_permission_to_dict(lkp_permdef_req)},
        )
    )
    assert role_permissions.filter == lkp_permdef_req.filter
    assert role_permissions.limit == lkp_permdef_req.limit
    assert role_permissions.columns == lkp_permdef_req.columns
    assert role_permissions.allow_aggregations == lkp_permdef_req.allow_aggregations
    assert role_permissions.computed_fields == lkp_permdef_req.computed_fields


def setup_inactive_roles_have_no_permissions() -> list:
    inactive_roles_list = []
    for role_name, role_metadata in roles_metadata().items():
        if role_metadata["is_active"] is False:
            inactive_roles_list.append(role_name)
    return inactive_roles_list


@pytest.mark.parametrize("inactive_role", setup_inactive_roles_have_no_permissions())
def test_inactive_roles_have_no_permissions(
    inactive_role: str, tables_and_roles_hash_dict: dict
):
    for table_name in tables_and_roles_hash_dict:
        assert inactive_role not in tables_and_roles_hash_dict[table_name]["roles"]


def setup_private_tables_only_accessible_by_owner_and_superuser() -> list:
    rules = domain_rules_metadata()
    private_table_rules = rules[
        "test_private_tables_only_accessible_by_owner_and_superusers"
    ]["private_roles"]
    testcases = []
    for private_role in private_table_rules:
        logging.info(private_role)
        analogous_private_roles = private_table_rules[private_role]["analogous_roles"]
        private_tables = private_table_rules[private_role]["private_tables"]
        for private_table in private_tables:
            testcases.append((analogous_private_roles, private_table))
    return testcases


@pytest.fixture(scope="session")
def superuser_roles(domain_rules: dict):
    return domain_rules[
        "test_private_tables_only_accessible_by_owner_and_superusers"
    ]["superuser_roles"]


@pytest.mark.parametrize(
    "analogous_private_roles, private_table",
    setup_private_tables_only_accessible_by_owner_and_superuser(),
)
def test_private_tables_only_accessible_by_owner_and_superuser(
    analogous_private_roles: list,
    private_table: str,
    tables_and_roles_hash_dict: dict,
    superuser_roles: list,
):
    roles_allowed_to_access_private_table = set(analogous_private_roles).union(
        set(superuser_roles)
    )
    assert (
        set(tables_and_roles_hash_dict[private_table]["roles"])
        - roles_allowed_to_access_private_table
        == set()
    )
