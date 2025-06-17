import sys
import os
import yaml
import json
from pprint import pprint
from typing import List
from typing import Dict
import glob
import ast
import util_filepath_and_fileloader as file_utils
import util_postgres_query as pg_helper

"""
ASSUMPTIONS:
- All API Objects that have permissions metadata must have test coverage
  - to verify that the granted permissions meet technical requirements,
  - to check for permission regressions,
- Each API Object has a single corresponding .feature file (that contains test scenarios that apply to that API object)
  - the feature file name is exactly the same as the API object name as seen in the API.

The matching naming convention of the file name and API object name will be the basis
of how this checker will determine permissions test coverage
"""

# TODO: needs alias_table_name resolution implemented


def get_permission_e2e_test_feature_files() -> List[str]:
    permissions_e2e_test_dir = file_utils.end_to_end_tests_root_dir()
    permissions_feature_files = [
        os.path.basename(x).replace(".feature", "") for x in glob.glob(permissions_e2e_test_dir + "*.feature")
    ]
    permissions_feature_files.sort()
    return permissions_feature_files


def get_relationship_e2e_test_feature_files() -> List[str]:
    relationships_e2e_test_dir = file_utils.end_to_end_tests_root_dir()
    relationships_feature_files = [
        os.path.basename(x).replace(".feature", "") for x in glob.glob(relationships_e2e_test_dir + "*.feature")
    ]
    return relationships_feature_files


def exposed_api_object_alias() -> Dict:
    res = pg_helper.run_postgres_query(
        """
        SELECT
        table_name,
        configuration ->> 'custom_root_fields' AS table_name_alias
        FROM
        hdb_catalog.hdb_table
    """
    )
    table_name_alias_lookup_dict = {}
    for row in res:
        if ast.literal_eval(row["table_name_alias"]) == {}:
            table_name_alias_lookup_dict[row["table_name"]] = row["table_name"]
        else:
            table_name_alias_lookup_dict[row["table_name"]] = ast.literal_eval(row["table_name_alias"])["select"]
    return table_name_alias_lookup_dict


def get_api_objects_with_permission_metadata() -> List[str]:
    res = pg_helper.run_postgres_query(
        """
        SELECT
        distinct table_name
        FROM
        hdb_catalog.hdb_permission
    """
    )
    table_name_alias_lookup_dict = exposed_api_object_alias()
    api_objs_with_perms = [table_name_alias_lookup_dict[row["table_name"]] for row in res]
    return api_objs_with_perms


def check_permissions_e2e_test_coverage():
    permissions_feature_files = get_permission_e2e_test_feature_files()
    api_objects_with_permission_metadata = get_api_objects_with_permission_metadata()
    no_test_coverage_api_objs = set(api_objects_with_permission_metadata) - set(permissions_feature_files)
    missing_coverage_table_count = len(no_test_coverage_api_objs)
    print(
        "The following {} API Objects are missing permissions E2E test coverage:\n".format(missing_coverage_table_count)
    )
    pprint(no_test_coverage_api_objs)


def check_hasura_permissions_regressions():
    permissions_feature_files = get_permission_e2e_test_feature_files()
    api_objects_with_permission_metadata = get_api_objects_with_permission_metadata()
    api_objs_with_e2e_tests_but_missing_in_hdb_permission = set(permissions_feature_files) - set(api_objects_with_permission_metadata)
    tables_with_permissions_regression_count = len(api_objs_with_e2e_tests_but_missing_in_hdb_permission)
    print(
        "The following {} API Objects have e2e tests but no data in hdb_catalog.hdb_permission:\n".format(tables_with_permissions_regression_count)
    )
    pprint(api_objs_with_e2e_tests_but_missing_in_hdb_permission)


if __name__ == "__main__":
    check_permissions_e2e_test_coverage()
    check_hasura_permissions_regressions()