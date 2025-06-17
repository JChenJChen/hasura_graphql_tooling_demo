from os import path, remove
import filecmp

import yaml

from hasura_tooling.remote_schema import (
    ROLES_WITH_OLD_DEFAULT_PERMISSIONS,
    AddressRemoteSchema,
)
from hasura_tooling.util_yaml_dumper import dump_remote_schema_metadata_to_yaml_file


def load_test_yamls(yaml_file: str, yaml_directory: str):
    with open(
        path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            f"{yaml_directory}",
            f"{yaml_file}",
        ),
        "r",
    ) as f:
        return yaml.safe_load(f)


def load_remote_schema_yaml() -> str:
    return load_test_yamls(
        yaml_file="remote_schemas.yaml", yaml_directory="remote_schemas"
    )


def load_metadata_roles_yaml() -> str:
    return load_test_yamls(
        yaml_file="metadata_roles.yaml", yaml_directory="metadata"
    )


class TestAddressRemoteSchema:
    def test_address_remote_schema_works_with_full_schema(self):
        metadata = load_metadata_roles_yaml()
        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.add("test")
        address_remote_schema = AddressRemoteSchema(remote_schema_metadata=metadata)
        res = address_remote_schema.schema(role="test")
        assert res
        assert "type Address" in res
        assert "type AutocompleteLatLongResult" in res
        assert "type GeoPoint" in res
        assert "input AutocompleteInput" in res
        assert "input AutocompleteLatLongInput" in res
        assert "input Coordinates" in res
        assert "input GeoBoundingBox" in res
        assert "input GeoPointInput" in res

        ROLES_WITH_OLD_DEFAULT_PERMISSIONS.remove("test")

    def test_address_remote_schema_works_with_limited_schema(self):
        metadata = load_remote_schema_yaml()
        address_remote_schema = AddressRemoteSchema(remote_schema_metadata=metadata)
        res = address_remote_schema.schema(role="test")
        assert res
        assert "type Address" in res
        assert "type AutocompleteLatLongResult" not in res
        assert "type GeoPoint" not in res
        assert "input AutocompleteInput" not in res
        assert "input AutocompleteLatLongInput" not in res
        assert "input Coordinates" not in res
        assert "input GeoBoundingBox" not in res
        assert "input GeoPointInput" not in res
