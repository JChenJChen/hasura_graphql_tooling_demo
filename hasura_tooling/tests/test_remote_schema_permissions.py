from os import path

import yaml

from hasura_tooling.remote_schema import (
    FULL_PERMISSIONS,
    LIMITED_PERMISSIONS,
    ROLES_WITH_OLD_DEFAULT_PERMISSIONS,
)
from hasura_tooling.remote_schema_permissions import (
    add_remote_schema_permissions_by_role,
    remove_remote_schema_permissions_by_role,
    update_e2e_remote_schema_tests_by_role,
)


def load_test_metadata():
    with open(
        path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            "remote_schemas",
            "remote_schemas.yaml",
        ),
        "r",
    ) as f:
        return yaml.safe_load(f)


def load_test_perm_metadata():
    with open(
        path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            "remote_schemas",
            "hasura_perm_metadata.yaml",
        ),
        "r",
    ) as f:
        return yaml.safe_load(f)


class TestRemoteSchemaPermissions:
    def test_new_role_gets_limited_permissions(self):
        metadata = load_test_metadata()
        res = add_remote_schema_permissions_by_role("test", metadata)
        address_metadata = [schema for schema in res if schema["name"] == "address"][0]

        assert address_metadata["permissions"]
        assert address_metadata["permissions"][0]["role"] == "test"
        assert (
            address_metadata["permissions"][0]["definition"]["schema"]
            == LIMITED_PERMISSIONS
        )

    def test_existing_role_is_not_duplicated(self):
        metadata = load_test_metadata()
        res = add_remote_schema_permissions_by_role("test", metadata)
        res = add_remote_schema_permissions_by_role("test", metadata)
        address_metadata = [schema for schema in res if schema["name"] == "address"][0]

        assert address_metadata["permissions"]
        assert len(address_metadata["permissions"]) == 1
        assert address_metadata["permissions"][0]["role"] == "test"
        assert (
            address_metadata["permissions"][0]["definition"]["schema"]
            == LIMITED_PERMISSIONS
        )

    def test_role_with_change_in_permissions_to_full(self):
        metadata = load_test_metadata()
        res = add_remote_schema_permissions_by_role("test", metadata)
        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.add("test")
        res = add_remote_schema_permissions_by_role("test", metadata)
        address_metadata = [schema for schema in res if schema["name"] == "address"][0]

        assert address_metadata["permissions"]
        assert address_metadata["permissions"][0]["role"] == "test"
        assert (
            address_metadata["permissions"][0]["definition"]["schema"]
            == FULL_PERMISSIONS
        )

        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.remove("test")

    def test_role_with_change_in_permissions_to_limited(self):
        metadata = load_test_metadata()
        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.add("test")
        res = add_remote_schema_permissions_by_role("test", metadata)
        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.remove("test")
        res = add_remote_schema_permissions_by_role("test", metadata)
        address_metadata = [schema for schema in res if schema["name"] == "address"][0]

        assert address_metadata["permissions"]
        assert address_metadata["permissions"][0]["role"] == "test"
        assert (
            address_metadata["permissions"][0]["definition"]["schema"]
            == LIMITED_PERMISSIONS
        )

    def test_two_new_roles_get_separate_limited_permissions(self):
        metadata = load_test_metadata()
        res = add_remote_schema_permissions_by_role("test1", metadata)
        res = add_remote_schema_permissions_by_role("test2", metadata)
        address_metadata = [schema for schema in res if schema["name"] == "address"][0]

        assert address_metadata["permissions"]
        assert address_metadata["permissions"][0]["role"] == "test1"
        assert (
            address_metadata["permissions"][0]["definition"]["schema"]
            == LIMITED_PERMISSIONS
        )
        assert address_metadata["permissions"][1]["role"] == "test2"
        assert (
            address_metadata["permissions"][1]["definition"]["schema"]
            == LIMITED_PERMISSIONS
        )

    def test_permission_for_role_is_removed(self):
        metadata = load_test_metadata()
        res = add_remote_schema_permissions_by_role("test", metadata)
        res = remove_remote_schema_permissions_by_role("test", metadata)
        address_metadata = list(filter(lambda x: x["name"] == "address", res))[0]

        assert not address_metadata["permissions"]

    def test_permission_for_role_removed_again_is_ok(self):
        metadata = load_test_metadata()
        res = add_remote_schema_permissions_by_role("test", metadata)
        res = remove_remote_schema_permissions_by_role("test", metadata)
        res = remove_remote_schema_permissions_by_role("test", metadata)
        address_metadata = [schema for schema in res if schema["name"] == "address"][0]

        assert not address_metadata["permissions"]

    def test_perm_metadata_for_new_role_limited_permissions(self):
        metadata = load_test_metadata()
        add_remote_schema_permissions_by_role("test", metadata)

        hasura_perm_metadata = update_e2e_remote_schema_tests_by_role(
            "test", False, load_test_perm_metadata()
        )
        expected = {
            "address": {
                "address_pos": [
                    {"address_pos": "test"},
                ],
                "address_neg": [],
            },
            "address_autocomplete": {
                "address_autocomplete_pos": [],
                "address_autocomplete_neg": [
                    {
                        "address_autocomplete_neg": "test",
                    }
                ],
            },
        }
        assert hasura_perm_metadata["address"] == expected["address"]
        assert (
            hasura_perm_metadata["address_autocomplete"]
            == expected["address_autocomplete"]
        )


    def test_perm_metadata_for_new_role_full_permissions(self):
        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.add("test")
        metadata = load_test_metadata()
        add_remote_schema_permissions_by_role("test", metadata)

        hasura_perm_metadata = update_e2e_remote_schema_tests_by_role(
            "test", False, load_test_perm_metadata()
        )
        expected = {
            "address": {
                "address_pos": [
                    {"address_pos": "test"},
                ],
                "address_neg": [],
            },
            "address_autocomplete": {
                "address_autocomplete_pos": [
                    {"address_autocomplete_pos": "test"},
                ],
                "address_autocomplete_neg": [],
            },
        }
        assert hasura_perm_metadata["address"] == expected["address"]
        assert (
            hasura_perm_metadata["address_autocomplete"]
            == expected["address_autocomplete"]
        )


        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.remove("test")

    def test_perm_metadata_for_new_role_limited_permissions_gets_removed(self):
        metadata = load_test_metadata()
        add_remote_schema_permissions_by_role("test", metadata)

        hasura_perm_metadata = update_e2e_remote_schema_tests_by_role(
            "test", False, load_test_perm_metadata()
        )
        hasura_perm_metadata = update_e2e_remote_schema_tests_by_role(
            "test", True, hasura_perm_metadata
        )

        expected = {
            "address": {"address_pos": [], "address_neg": []},
            "address_autocomplete": {
                "address_autocomplete_pos": [],
                "address_autocomplete_neg": [],
            },
        }
        assert hasura_perm_metadata["address"] == expected["address"]
        assert (
            hasura_perm_metadata["address_autocomplete"]
            == expected["address_autocomplete"]
        )
