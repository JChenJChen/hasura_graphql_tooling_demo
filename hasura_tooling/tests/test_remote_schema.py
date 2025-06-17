from os import path, remove
import filecmp

import yaml

from hasura_tooling.remote_schema import (
    ROLES_WITH_OLD_DEFAULT_PERMISSIONS,
    AddressRemoteSchema,
    PiplRemoteSchema,
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


class TestPiplRemoteSchema:
    def test_pipl_remote_schema_works_for_schema(self):
        metadata = load_remote_schema_yaml()
        pipl_remote_schema = PiplRemoteSchema(remote_schema_metadata=metadata)
        res = pipl_remote_schema.schema()
        assert res
        assert "type PiplSearchResponse" in res
        assert "pipl_search" in res
        assert "scalar jsonb" in res

    def test_add_permissions_if_api_remote_schema_is_set(self):
        metadata = load_remote_schema_yaml()
        test_roles = load_metadata_roles_yaml()
        pipl_remote_schema = PiplRemoteSchema(
            remote_schema_metadata=metadata, metadata_roles=test_roles
        )
        remote_metadata = pipl_remote_schema.add_permission(role="test")

        assert len(remote_metadata) == 2
        assert "pipl" == remote_metadata[1]["name"]
        assert len(remote_metadata[1]["permissions"]) == 1
        assert remote_metadata[1]["permissions"][0]["role"] == "test"
        assert remote_metadata[1]["permissions"][0]["role"] != "test2"
        assert (
            "type PiplSearchResponse"
            in remote_metadata[1]["permissions"][0]["definition"]["schema"]
        )

    def test_remove_permissions(self):
        metadata = load_remote_schema_yaml()
        test_roles = load_metadata_roles_yaml()
        pipl_remote_schema = PiplRemoteSchema(
            remote_schema_metadata=metadata, metadata_roles=test_roles
        )
        remote_metadata = pipl_remote_schema.add_permission(role="test")
        remote_metadata = pipl_remote_schema.remove_permission(role="test")

        assert len(remote_metadata) == 2
        assert "pipl" == remote_metadata[1]["name"]
        assert len(remote_metadata[1]["permissions"]) == 0

    def test_yaml_output_file_is_formatted_correctly_for_address(self):
        metadata = load_remote_schema_yaml()
        address_remote_schema = AddressRemoteSchema(remote_schema_metadata=metadata)
        metadata = address_remote_schema.add_permission("test_yaml")

        formatted_file = path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            "remote_schemas",
            "remote_schemas_format_test_formatted_file_for_address.yaml",
        )
        output_file = path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            "remote_schemas",
            "remote_schemas_format_test_output_file_for_address.yaml",
        )

        with open(output_file, "w") as f:
            dump_remote_schema_metadata_to_yaml_file(metadata=metadata, yaml_file=f)

        res = filecmp.cmp(formatted_file, output_file, shallow=False)

        assert res

        remove(output_file)

    def test_yaml_output_file_is_formatted_correctly_for_pipl(self):
        metadata = load_remote_schema_yaml()
        test_roles = load_metadata_roles_yaml()
        pipl_remote_schema = PiplRemoteSchema(
            remote_schema_metadata=metadata, metadata_roles=test_roles
        )
        metadata = pipl_remote_schema.add_permission("test")

        formatted_file = path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            "remote_schemas",
            "remote_schemas_format_test_formatted_file_for_pipl.yaml",
        )
        output_file = path.join(
            path.dirname(path.abspath(__file__)),
            "test_metadata",
            "remote_schemas",
            "remote_schemas_format_test_output_file_for_pipl.yaml",
        )

        with open(output_file, "w") as f:
            dump_remote_schema_metadata_to_yaml_file(metadata=metadata, yaml_file=f)

        res = filecmp.cmp(formatted_file, output_file, shallow=False)

        assert res

        remove(output_file)
