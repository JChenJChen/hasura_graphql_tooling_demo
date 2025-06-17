import logging
from abc import ABC
from operator import itemgetter
from typing import List, Dict, Any

from hasura_tooling.util_filepath_and_fileloader import (
    remote_schemas_metadata,
    roles_metadata,
)
from hasura_tooling.util_yaml_dumper import create_literal_scalar_string


ROLES_WITH_OLD_DEFAULT_PERMISSIONS = set(
    [
        role
        for role, metadata in roles_metadata().items()
        if metadata["remote_schema_permissions"] == "full"
    ]
)


FULL_PERMISSIONS = create_literal_scalar_string(
    """\
type Address {
	city: String
	one_line_address: String
	state: String
	street_name: String
	street_number: String
	zip: String
}
input AutocompleteInput {
	centerpoint: Coordinates
	partial_address: String!
}
input AutocompleteLatLongInput {
	bounding_box: GeoBoundingBox
	partial_address: String!
}
type AutocompleteLatLongResult {
	city: String
	lat_long: GeoPoint
	one_line_address: String
	state: String
	street_name: String
	street_number: String
	unit_number: String
	unit_number_prefix: String
	zip: String
}
input Coordinates {
	latitude: Float!
	longitude: Float!
}
input GeoBoundingBox {
	northeast: GeoPointInput!
	southwest: GeoPointInput!
}
type GeoPoint {
	latitude: Float!
	longitude: Float!
}
input GeoPointInput {
	latitude: Float!
	longitude: Float!
}
type Query {
	address(address: String): Address
}
"""
)


LIMITED_PERMISSIONS = create_literal_scalar_string(
    """\
type Address {
	city: String
	one_line_address: String
	state: String
	street_name: String
	street_number: String
	zip: String
}
type Query {
	address(address: String): Address
}
"""
)


class RemoteSchema(ABC):
    remote_schema_key = "api_remote_schemas"

    def __init__(self, remote_schema_metadata: List[Dict[str, str]] = None) -> None:
        self._remote_schema_metadata = (
            remote_schema_metadata or remote_schemas_metadata()
        )

    @property
    def remote_metadata(self) -> List[Dict[str, str]]:
        return self._remote_schema_metadata

    @property
    def metadata(self):
        return [
            schema for schema in self.remote_metadata if schema["name"] == self.name
        ][0]

    def sort_permissions(self) -> List[Dict[str, str]]:
        return self.metadata["permissions"].sort(key=itemgetter("role"))

    def remove_permission(self, role: str) -> List[Dict[str, str]]:
        self.metadata["permissions"] = [
            permissions
            for permissions in self.metadata["permissions"]
            if permissions["role"] != role
        ]

        self.sort_permissions()
        return self.remote_metadata

    def does_role_permission_exist(self, role: str) -> bool:
        for permissions in self.metadata["permissions"]:
            if permissions["role"] == role:
                return True

        return False


class AddressRemoteSchema(RemoteSchema):
    name = "address"

    def __init__(self, remote_schema_metadata: List[Dict[str, str]] = None) -> None:
        super().__init__(remote_schema_metadata=remote_schema_metadata)

    def schema(self, role: str) -> str:
        if role in ROLES_WITH_OLD_DEFAULT_PERMISSIONS:
            return FULL_PERMISSIONS
        return LIMITED_PERMISSIONS

    def add_permission(self, role: str) -> List[Dict[str, str]]:
        permissions_exist_for_role = False
        schema = self.schema(role=role)

        for permission in self.metadata["permissions"]:
            if permission["role"] == role:
                permission["definition"]["schema"] = schema
                permissions_exist_for_role = True
                break

        if not permissions_exist_for_role:
            self.metadata["permissions"].append(
                {"role": role, "definition": {"schema": schema}}
            )

        self.sort_permissions()
        return self.remote_metadata


    def get_roles(self, metadata_roles: Dict[str, Any] = None) -> List[str]:
        roles = []
        metadata_roles = metadata_roles or roles_metadata()

        for role, metadata in metadata_roles.items():
            try:
                for r_s in metadata["api_remote_schemas"]:
                    if r_s == self.name:
                        roles.append(role)
            except KeyError as e:
                continue

        return roles

    def add_permission(self, role: str) -> List[Dict[str, str]]:

        # The role needs to be included in the `api_remote_schemas` (in metadata_roles.yaml)
        if role not in self.roles:
            logging.info(
                f"The role, {role}, is not included for the remote schema, {self.name}"
            )
            return self.remote_metadata

        if self.does_role_permission_exist(role=role):
            logging.info(f"Will override permissioning for {role}")
            self.remove_permission(role=role)

        self.metadata["permissions"].append(
            {"role": role, "definition": {"schema": self.schema()}}
        )

        self.sort_permissions()
        return self.remote_metadata
