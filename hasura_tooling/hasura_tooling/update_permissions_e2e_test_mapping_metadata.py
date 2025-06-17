import yaml
from hasura_tooling.util_filepath_and_fileloader import (
    permissions_e2e_tests_mapping_metadata,
    permissions_e2e_tests_mapping_metadata_filepath,
)
from hasura_tooling.lookup_alias_by_actual_table_name import (
    translate_actual_table_name_to_alias,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def update_permissions_e2e_test_mapping_metadata_by_table(
    roles: list, table_names: list, to_pos: bool
):
    """
    This tooling:
        - updates the permissions e2e test mapping metadata for a list of specified tables and roles.
        - is integrated with all permission update functionalities in the Hasura metadata update tooling.
        - will append a mapping to the "_pos" positive access confirmation test scenario for each table and role,
            and if a contradictory "_neg" negative access confirmation mapping exists for a role on a table, it
            will remove it to prevent erroneous double mapping.

    Args:
        roles (list): roles that have access to the list of specified tables.
        table_names (list): tables that the specified roles have access to.
    """
    perm_e2e_test_mapping_metadata = permissions_e2e_tests_mapping_metadata()
    e2e_test_exempt_roles = ["qa"]
    for table in table_names:
        table = translate_actual_table_name_to_alias(table)
        metadata_pos_key = str(table) + "_pos"
        metadata_neg_key = str(table) + "_neg"
        if table not in perm_e2e_test_mapping_metadata.keys():
            perm_e2e_test_mapping_metadata[table] = {
                metadata_pos_key: [],
                metadata_neg_key: [],
            }
        pos_keys = [
            k
            for k in perm_e2e_test_mapping_metadata[table].keys()
            if k.endswith("_pos")
        ]
        mapped_to_pos = False
        for role in roles:
            if role not in e2e_test_exempt_roles:
                # adds role to the _pos test case if mapping doesn't exist already in any _pos groups
                if to_pos:
                    try:
                        for pos_key in pos_keys:
                            if {pos_key: role} in perm_e2e_test_mapping_metadata[table][
                                pos_key
                            ]:
                                mapped_to_pos = True
                        if not mapped_to_pos:
                            perm_e2e_test_mapping_metadata[table][
                                metadata_pos_key
                            ].append({metadata_pos_key: role})
                            print(
                                "Added mapping: {role} to {metadata_pos_key}".format(
                                    role=role, metadata_pos_key=metadata_pos_key
                                )
                            )
                    except KeyError:
                        print(
                            "{metadata_pos_key} does not exist in hasura_perm_metadata.yaml. \
                            Does it need to be created?".format(
                                metadata_pos_key=metadata_pos_key
                            )
                        )
                    try:
                        # then removes role from the _neg test case if that was previously mapped
                        if {metadata_neg_key: role} in perm_e2e_test_mapping_metadata[
                            table
                        ][metadata_neg_key]:
                            perm_e2e_test_mapping_metadata[table][
                                metadata_neg_key
                            ].remove({metadata_neg_key: role})
                            print(
                                "Removed mapping: {role} to {metadata_neg_key}".format(
                                    role=role, metadata_neg_key=metadata_neg_key
                                )
                            )
                    except KeyError:
                        print(
                            "{metadata_neg_key} does not exist in hasura_perm_metadata.yaml. \
                            Does it need to be created?".format(
                                metadata_neg_key=metadata_neg_key
                            )
                        )
                else:
                    try:
                        if {
                            metadata_neg_key: role
                        } not in perm_e2e_test_mapping_metadata[table][
                            metadata_neg_key
                        ]:
                            perm_e2e_test_mapping_metadata[table][
                                metadata_neg_key
                            ].append({metadata_neg_key: role})
                            print(
                                "Added mapping: {role} to {metadata_neg_key}".format(
                                    role=role, metadata_neg_key=metadata_neg_key
                                )
                            )
                    except KeyError:
                        print(
                            "{metadata_neg_key} does not exist in hasura_perm_metadata.yaml. \
                            Does it need to be created?".format(
                                metadata_neg_key=metadata_neg_key
                            )
                        )
                    try:
                        # then removes role from the _pos test case if that was previously mapped
                        if {metadata_pos_key: role} in perm_e2e_test_mapping_metadata[
                            table
                        ][metadata_pos_key]:
                            perm_e2e_test_mapping_metadata[table][
                                metadata_pos_key
                            ].remove({metadata_pos_key: role})
                            print(
                                "Removed mapping: {role} to {metadata_pos_key}".format(
                                    role=role, metadata_pos_key=metadata_pos_key
                                )
                            )
                    except KeyError:
                        print(
                            "{metadata_pos_key} does not exist in hasura_perm_metadata.yaml. \
                            Does it need to be created?".format(
                                metadata_pos_key=metadata_pos_key
                            )
                        )
    with open(permissions_e2e_tests_mapping_metadata_filepath(), "w") as f:
        yaml.dump(
            perm_e2e_test_mapping_metadata,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


def remove_permissions_e2e_test_mapping_metadata_by_table(
    roles: list, table_names: list
):
    """
    Args:
        roles: list of roles to remove permissions for
        table_names: list of tables to remove permissions for. If empty list, then remove all permissions.
    """
    perm_e2e_test_mapping_metadata = permissions_e2e_tests_mapping_metadata()
    for k_top_level, v_top_level in perm_e2e_test_mapping_metadata.items():
        # if current table_name is in list of tables to delete permissions for, OR...
        # if table_names is an empty list, denoting all tables' permissions for the specified roles are to be deleted
        if k_top_level in table_names or not table_names:
            for k_2nd_level, v_2nd_level in v_top_level.items():
                for i in v_2nd_level:
                    for k, v in i.items():
                        if v in roles:
                            v_2nd_level.remove(i)
    with open(permissions_e2e_tests_mapping_metadata_filepath(), "w") as f:
        yaml.dump(
            perm_e2e_test_mapping_metadata,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )
