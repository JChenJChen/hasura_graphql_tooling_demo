import os.path
from hasura_tooling.util_filepath_and_fileloader import relationships_end_to_end_dir
from hasura_tooling.util_postgres_query import run_postgres_query
from hasura_tooling.util_introspection import get_subdir_by_table_name

"""[summary]




Sample input standardized relationship tooling metadata:

[
{'origin_table': 'table1', 'origin_column': 'column1', 'remote_table': 'table2',
    'remote_column': 'column2', 'rel_type': 'object', 'rel_name': 'name_of_relationship'},
]
"""


def relationship_e2e_feature_file_test_scenario_template(metadata_row: dict) -> str:
    rel_test_scenario_template = """


    @{origin_table}
    @{remote_table}
    @{relationship_name}
    @rel_type_{rel_type_tag}
    Scenario Outline: confirm {relationship_name} relationship work and is correct relationship type.

    * header Authorization = 'Bearer ' + tokens['<role>']
    Given def query = read('queries/{origin_table}/{relationship_name}.graphql')
    And request {{ query : '#(query)' }}
    When method post
    Then status 200

    * match $.data.{origin_table}[0].{relationship_name} {rel_eq}= '#[]'

    Examples:
    | role              |
    | test_role         |


    """
    if metadata_row["rel_type"] == "object":
        rel_eq = "!"
        rel_type_tag = "obj"
    elif metadata_row["rel_type"] == "array":
        rel_eq = "="
        rel_type_tag = "array"
    else:
        raise ValueError("invalid relation type")
    return rel_test_scenario_template.format(
        origin_table=metadata_row["origin_table"],
        remote_table=metadata_row["remote_table"],
        relationship_name=metadata_row["rel_name"],
        rel_type_tag=rel_type_tag,
        rel_eq=rel_eq,
    )


def feature_file_header(metadata_row: dict) -> str:
    return f"""
    Feature: {metadata_row["origin_table"]} as origin table
    Background:
    * url baseUrl + '/graphql'
    * def tokens = read('../tokens.yaml')['tokens'] """


def create_or_append_relationship_test_scenarios_in_feature_file(row: dict):
    # TODO: replace metadata txt file with a more elegant and robust implementation
    subdir = get_subdir_by_table_name(row["origin_table"])
    file_name = (
        os.path.join(relationships_end_to_end_dir(), subdir, row["origin_table"])
        + ".feature"
    )
    # write header (Feature + Background sections) if file doesn't exist
    output_file = open(file_name, "a")
    if os.path.isfile(file_name) is False:
        output_file.write(feature_file_header(row))
    else:  # if file exists already, don't write the header, just append the Scenario Outlines
        output_file_contents = open(file_name).read()
        if row["rel_name"] not in output_file_contents:
            # assumption: all relationship test scenarios are properly tagged, including a relationship_name tag,
            # which is used as a proxy to check if the relationship test scenario exists.
            output_file.write(relationship_e2e_feature_file_test_scenario_template(row))
        else:
            print(
                f"Test scenario for {row['rel_name']} already exists in {row['origin_table']}.feature -- \
                Skipping this row of metadata."
            )
    output_file.close()


def covert_columns_list_to_graphql_indentation(
    columns: list, is_origin_table: bool
) -> str:
    columns_formatted = ""
    for column in columns:
        columns_formatted += str(column)
        if is_origin_table:
            columns_formatted += "\n            "
        else:
            columns_formatted += "\n        "
    return columns_formatted


def relationship_e2e_graphql_file_query_template(row):
    return (
        "{\n    %s {\n        %s {\n            %s\n        }\n        %s\n    }\n}\n"
        % (
            row["origin_table"],
            row["rel_name"],
            covert_columns_list_to_graphql_indentation(
                get_unique_or_queryable_columns(row["remote_table"], "test_role"), True
            ),
            covert_columns_list_to_graphql_indentation(
                get_unique_or_queryable_columns(row["origin_table"], "test_role"), False
            ),
        )
    )


def get_unique_or_queryable_columns(table_name: str, role_name: str) -> list:
    queryable_columns_query = """
        SELECT
        perm_def -> 'columns' AS queryable_columns
        FROM
        hdb_catalog.hdb_permission
        WHERE
        table_name = '{table_name}' AND
        role_name = '{role_name}';
    """.format(
        table_name=table_name, role_name=role_name
    )
    queryable_columns = run_postgres_query(queryable_columns_query)
    all_columns = run_postgres_query(
        """
        SELECT DISTINCT
        column_name as all_columns
        FROM
        information_schema.columns;
    """
    )
    # print("queryable_columns:")
    # print(queryable_columns)
    # print("=============================")
    # print("all_columns:")
    # print(all_columns)
    unique_columns = list(
        set(queryable_columns[0]["queryable_columns"])
        - set(all_columns[0]["all_columns"])
    )
    if len(unique_columns) > 0:
        return unique_columns
    else:
        return queryable_columns[0]["queryable_columns"]


def create_relationship_e2e_test_graphql_file(row: dict):
    filled_in = relationship_e2e_graphql_file_query_template(row)
    subdir = get_subdir_by_table_name(row["origin_table"])
    origin_table_query_directory = os.path.join(
        relationships_end_to_end_dir(), subdir, "queries", row["origin_table"]
    )
    relationship_name = row["rel_name"]
    file_name = f"{origin_table_query_directory}/{relationship_name}.graphql"
    if not os.path.exists(file_name):
        if not os.path.exists(origin_table_query_directory):
            os.makedirs(origin_table_query_directory)
        output_file = open(file_name, "w")
        output_file.write(filled_in)
        output_file.close()
    else:
        print(
            f"query file for {relationship_name} exists already. Skipping this row of metadata."
        )


def main(row: dict):
    create_or_append_relationship_test_scenarios_in_feature_file(row)
    create_relationship_e2e_test_graphql_file(row)
