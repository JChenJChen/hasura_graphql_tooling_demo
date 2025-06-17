import yaml
import ast

from hasura_tooling.util_filepath_and_fileloader import (
    hasura_metadata_tables,
    _get_repo_rootdir,
    relationships_tooling_input_filepath,
    tables_metadata_filepath,
)
from hasura_tooling.create_or_append_relationship_e2e_tests import main
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


# TODO: Add validation to ensure origin table.column & remote table.column both exist.


def fill_relationship_template_from_metadata(row: dict) -> dict:
    # fills in relationship metadata block template with a row of input metadata values
    remote_table = row["remote_table"]
    origin_column = row["origin_column"]
    remote_column = row["remote_column"]
    relationship_type = row["rel_type"]
    relationship_name = row["rel_name"]
    if relationship_type not in ["array", "object"]:
        raise ValueError("Invalid relationship type entered")
    create_relationship_template = {
        "name": relationship_name,
        "using": {
            "manual_configuration": {
                "column_mapping": {origin_column: remote_column},
                "remote_table": {"name": remote_table, "schema": "public"},
            }
        },
    }
    return create_relationship_template


def check_if_relationship_metadata_already_exists(
    relationship: dict, row: dict
) -> bool:
    """Check to see if to-be-added relationship metadata block already exists in metadata

    Args:
        relationship (dict): relationship metadata block in API metadata.
        row (dict): relationship creation input values.

    Returns:
        bool: If relationship to be created already exists in API metadata.
    """
    metadata_origin_column = list(
        relationship["using"]["manual_configuration"]["column_mapping"].keys()
    )[0]
    metadata_remote_column = relationship["using"]["manual_configuration"][
        "column_mapping"
    ][metadata_origin_column]
    if (
        relationship["name"] == row["rel_name"]
        and relationship["using"]["manual_configuration"]["remote_table"]["name"]
        == row["remote_table"]
        and metadata_origin_column == row["origin_column"]
        and metadata_remote_column == row["remote_column"]
    ):
        return True
    else:
        return False


def orchestrator():
    hasura_tables_metadata = hasura_metadata_tables()
    # TODO: replace input metadata text file with something more robust
    metadata = ast.literal_eval(open(relationships_tooling_input_filepath()).read())
    for row in metadata:
        if row["rel_type"] not in ["array", "object"]:
            raise ValueError("Invalid relationship type (wasn't array or object)")
        else:
            relationship_metadata_block = fill_relationship_template_from_metadata(row)
            print(relationship_metadata_block)
            for table in hasura_tables_metadata:
                # find origin_table to insert relationship metadata block
                if table["table"]["name"] == row["origin_table"]:
                    exists = False
                    rel_type_key = "{rel_type}_relationships".format(
                        rel_type=row["rel_type"]
                    )
                    for relationship in table[rel_type_key]:
                        if check_if_relationship_metadata_already_exists(
                            relationship, row
                        ):
                            print(
                                "Relationship metadata already exists for: \n Input values:"
                            )
                            print(row)
                            print("-------------\n In metadata:")
                            print(relationship)
                            exists = True
                    if not exists:
                        # appends relationship metadata to tables.yaml
                        table[rel_type_key].append(relationship_metadata_block)
            main(row)
    with open(tables_metadata_filepath(), "w") as f:
        yaml.dump(
            hasura_tables_metadata,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


if __name__ == "__main__":
    orchestrator()
