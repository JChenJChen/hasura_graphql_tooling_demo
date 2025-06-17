from hasura_tooling.util_postgres_query import run_postgres_query
from hasura_tooling.util_filepath_and_fileloader import end_to_end_tests_root_dir
import sys


def get_relationship_details(origin_table_name, relationship_name) -> dict:
    query = """
        SELECT
                t.origin_table,
                replace(cast(t.remote_table as text),'"','') as remote_table,
                t.rel_name,
                t.rel_type,
                t.key   AS origin_column,
                t.value AS remote_column
                FROM
                (
                    SELECT
                    table_name                                                                 AS origin_table,
                    rel_def -> 'manual_configuration' -> 'remote_table' -> 'name'              AS remote_table,
                    rel_name,
                    rel_type,
                    (jsonb_each_text(rel_def -> 'manual_configuration' -> 'column_mapping')).*
                    FROM
                    hdb_catalog.hdb_relationship
                    WHERE
                    rel_name = '{relationship_name}' and
                    table_name ~ '{table_name}') as t;
    """.format(
        relationship_name=relationship_name, table_name=origin_table_name
    )
    return run_postgres_query(query)[0]


def where_remote_table_not_null_clause(relationship_details: dict) -> str:
    print(relationship_details)
    where_clause_template = "(\n        where: {\n            %s: {\n                    %s :{_is_null:false}\n                }\n            }\n        )\n        "
    return where_clause_template % (
        relationship_details["rel_name"],
        relationship_details["remote_column"],
    )


def main():
    query_file_subdir_path = sys.argv[1]
    relationships_test_query_dir = (
        end_to_end_tests_root_dir() + "relationships/queries/"
    )
    graphql_file = relationships_test_query_dir + query_file_subdir_path
    try:
        with open(graphql_file, "r") as q:
            query_text = q.read()
            curly_bracket_position = -1
            for i in range(0, 3):
                curly_bracket_position = query_text.find(
                    "{", curly_bracket_position + 1
                )
                if i == 0:
                    start_bracket_position = curly_bracket_position
                elif i == 1:
                    origin_table_bracket_position = curly_bracket_position
                else:
                    remote_table_bracket_position = curly_bracket_position
            origin_table_name = query_text[
                start_bracket_position + 1 : origin_table_bracket_position
            ].strip()
            relationship_name = query_text[
                origin_table_bracket_position + 1 : remote_table_bracket_position
            ].strip()
            relationship_details = get_relationship_details(
                origin_table_name, relationship_name
            )
            where_clause_insert = where_remote_table_not_null_clause(
                relationship_details
            )
            updated_query = (
                query_text[:origin_table_bracket_position]
                + where_clause_insert  # noqa: W503
                + query_text[origin_table_bracket_position:]  # noqa: W503
            )
            print(updated_query)
            output_file = open(graphql_file, "w")
            output_file.write(updated_query)
            output_file.close()
    except IOError:
        print("File read error:{graphql_file}".format(graphql_file=graphql_file))


if __name__ == "__main__":
    main()
