import yaml
import os
from typing import Dict, Any

from hasura_tooling.util_filepath_and_fileloader import (
    sharded_tables_dir,
)

# TODO: consider removing get_print_dict_case_keys(), case-specific diff dict keys not functionally necessary.


def get_print_dict_case_keys(case: str) -> list:
    cases = {
        "role_perm_vs_prescription": ["role_actual_perms", "prescription"],
        "superset1_vs_superset2": ["superset_1", "superset_2"],
        "role1_perm_vs_role2_perm": ["role_1", "role_2"],
    }
    return cases[case]


def get_role_permdef_shards(role: str) -> dict:
    role_aggregated_permdef = {}
    sharded_tables_yaml_rootdir = sharded_tables_dir()
    role_file_name = role + ".yaml"
    for subdir, dirs, files in os.walk(sharded_tables_yaml_rootdir):
        if role_file_name in files:
            # parsing logic: table_name always follow `metadata/tables`.
            # This is safer than taking the last element blindly.
            table_name = subdir.split("/")[subdir.split("/").index("tables") + 1]
            with open(os.path.join(subdir, role_file_name), "r") as p:
                role_perm_def = yaml.safe_load(p)
                role_aggregated_permdef[table_name] = role_perm_def["permission"]
    return role_aggregated_permdef


def diff_2_perm_def_dicts(perm1: dict, perm2: dict, case: str) -> dict:
    """
    Helper function that diffs 2 sets of permissions -- can be a superset's permission definition, a role's actual
     permissions, or a role's permission prescription (union of role's sepersets' perm_defs).

    Inputs: 2 permissions dictionaries to be diff'd.
      In compare_role_perm_def_to_prescription(), it's a role's actual permissions vs. its prescribed permissions.
      In compare_two_superset_perm_defs(), it's 2 supersets' perm_defs.

    Output: A dictionary of 3 dictionaries:
      1. permissions present only in perm1 and absent in perm2.
      2. permissions present only in perm2 and absent in perm1.
      3. permissions present in both, but differ in definition (ex: columns, row limit, filter, etc).
    """
    only_in_perm1 = dict()
    only_in_perm2 = dict()
    in_both_diff = dict()
    # get_print_dict_case_keys contains what text to print based on which comparison case:
    # 1: compare role's actual permissions to its prescription (based on source-of-truth)
    # 2: compare 2 supersets' permissions-definitions
    # 3: compare 2 roles' actual permissions

    for perm1_tablename, perm1_permdef in perm1.items():
        try:
            perm2_permdef = perm2[perm1_tablename]
            # get columns diff
            table_in_both_diff_temp: Dict[str, Any] = dict()
            cols_only_in_perm1 = set(perm1_permdef["columns"]) - set(
                perm2_permdef["columns"]
            )
            if len(cols_only_in_perm1) > 0:
                table_in_both_diff_temp[
                    f"columns_only_in_{get_print_dict_case_keys(case)[0]}"
                ] = cols_only_in_perm1
            cols_only_in_perm2 = set(perm2_permdef["columns"]) - set(
                perm1_permdef["columns"]
            )
            if len(cols_only_in_perm2) > 0:
                table_in_both_diff_temp[
                    f"columns_only_in_{get_print_dict_case_keys(case)[1]}"
                ] = cols_only_in_perm2
            # get row limit diff
            if perm1_permdef["limit"] != perm2_permdef["limit"]:
                table_in_both_diff_temp["row_limit_diff"] = {
                    get_print_dict_case_keys(case)[0]: perm1_permdef["limit"],
                    get_print_dict_case_keys(case)[1]: perm2_permdef["limit"],
                }
            # get filter diff
            if perm1_permdef["filter"] != perm2_permdef["filter"]:
                table_in_both_diff_temp["filter_diff"] = {
                    get_print_dict_case_keys(case)[0]: perm1_permdef["filter"],
                    get_print_dict_case_keys(case)[1]: perm2_permdef["filter"],
                }
            # explicitly declare computed_fields as empty list if absent
            perm1_cf = perm1_permdef.get("computed_fields", [])
            perm2_cf = perm2_permdef.get("computed_fields", [])
            if perm1_cf != perm2_cf:
                table_in_both_diff_temp["computed_fields_diff"] = {
                    get_print_dict_case_keys(case)[0]: perm1_cf,
                    get_print_dict_case_keys(case)[1]: perm2_cf,
                }
            # explicitly declare allow_agg as false if absent
            perm1_ag = perm1_permdef.get("allow_aggregations", False)
            perm2_ag = perm2_permdef.get("allow_aggregations", False)
            if perm1_ag != perm2_ag:
                table_in_both_diff_temp["allow_aggregations_diff"] = {
                    get_print_dict_case_keys(case)[0]: perm1_ag,
                    get_print_dict_case_keys(case)[1]: perm2_ag,
                }
            if table_in_both_diff_temp != {}:
                in_both_diff[perm1_tablename] = table_in_both_diff_temp
        except KeyError:
            only_in_perm1[perm1_tablename] = perm1_permdef
    for perm2_table, perm2_permdef in perm2.items():
        if perm2_table not in perm1.keys():
            only_in_perm2[perm2_table] = perm2_permdef
    return {
        f"only_in_{get_print_dict_case_keys(case)[0]}": only_in_perm1,
        f"only_in_{get_print_dict_case_keys(case)[1]}": only_in_perm2,
        "in_both_diff": in_both_diff,
    }
