from operator import itemgetter
from typing import Dict, List
import logging

import yaml

from hasura_tooling.remote_schema import (
    ROLES_WITH_OLD_DEFAULT_PERMISSIONS,
    AddressRemoteSchema,
)
from hasura_tooling.util_filepath_and_fileloader import (
    remote_schemas_metadata,
    remote_schemas_filepath,
    permissions_e2e_tests_mapping_metadata_filepath,
    permissions_e2e_tests_mapping_metadata,
)
from hasura_tooling.util_yaml_dumper import (
    IndentedListYamlDumper,
    dump_remote_schema_metadata_to_yaml_file,
)


def remove_remote_schema_permissions(role: str) -> None:
    metadata_dict = remove_remote_schema_permissions_by_role(role)
    with open(remote_schemas_filepath(), "w") as f:
        dump_remote_schema_metadata_to_yaml_file(metadata_dict, f)
    update_e2e_remote_schema_tests(role, remove=True)


def remove_remote_schema_permissions_by_role(
    role: str, metadata: List[Dict] = None
) -> List[Dict]:
    logging.info(f"Removing remote schema permissions for {role}")
    remote_schema_metadata = AddressRemoteSchema(
        remote_schema_metadata=metadata
    ).remove_permission(role=role)
    return remote_schema_metadata


def add_remote_schema_permissions(role: str) -> None:
    metadata_dict = add_remote_schema_permissions_by_role(role)
    with open(remote_schemas_filepath(), "w") as f:
        dump_remote_schema_metadata_to_yaml_file(metadata_dict, f)
    update_e2e_remote_schema_tests(role, remove=False)


def add_remote_schema_permissions_by_role(
    role: str, metadata: List[Dict] = None
) -> List[Dict]:
    logging.info(f"Adding remote schema permissions for {role}")
    remote_schema_metadata = AddressRemoteSchema(
        remote_schema_metadata=metadata
    ).add_permission(role=role)
    return remote_schema_metadata


def update_e2e_remote_schema_tests(role: str, remove: bool = False) -> None:
    hasura_perm_metadata = update_e2e_remote_schema_tests_by_role(role, remove)
    with open(permissions_e2e_tests_mapping_metadata_filepath(), "w") as f:
        yaml.dump(
            hasura_perm_metadata,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


def update_e2e_remote_schema_tests_by_role(
    role: str, remove: bool = False, hasura_perm_metadata: Dict[str, Dict] = None
) -> Dict[str, Dict]:
    logging.info(f"Update e2e tests for remote schemas for {role}")
    if not hasura_perm_metadata:
        hasura_perm_metadata = permissions_e2e_tests_mapping_metadata()
    all_remote_schema_queries = set(
        ["address", "address_autocomplete"]
    )
    limited_remote_schema_queries = set(["address"])

    for remote_schema_query in all_remote_schema_queries:
        metadata_pos_key = f"{remote_schema_query}_pos"
        metadata_neg_key = f"{remote_schema_query}_neg"

        if remote_schema_query not in hasura_perm_metadata.keys():
            hasura_perm_metadata[remote_schema_query] = {
                metadata_pos_key: [],
                metadata_neg_key: [],
            }

        pos = hasura_perm_metadata[remote_schema_query][metadata_pos_key]
        neg = hasura_perm_metadata[remote_schema_query][metadata_neg_key]

        # remove any tests if we're removing the role
        if remove:
            if {metadata_pos_key: role} in pos:
                pos.remove({metadata_pos_key: role})
            if {metadata_neg_key: role} in neg:
                neg.remove({metadata_neg_key: role})
        else:
            if remote_schema_query in limited_remote_schema_queries:
                if {metadata_pos_key: role} not in pos:
                    pos.append({metadata_pos_key: role})
                if {metadata_neg_key: role} in neg:
                    neg.remove({metadata_neg_key: role})
            else:
                if role in ROLES_WITH_OLD_DEFAULT_PERMISSIONS:
                    if {metadata_pos_key: role} not in pos:
                        pos.append({metadata_pos_key: role})
                    if {metadata_neg_key: role} in neg:
                        neg.remove({metadata_neg_key: role})
                else:
                    if {metadata_neg_key: role} not in neg:
                        neg.append({metadata_neg_key: role})
                    if {metadata_pos_key: role} in pos:
                        pos.remove({metadata_pos_key: role})

    return hasura_perm_metadata
