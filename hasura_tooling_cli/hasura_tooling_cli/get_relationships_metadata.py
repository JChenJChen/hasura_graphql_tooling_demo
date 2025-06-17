import yaml
from pprint import pprint
import click
from typing import Dict

from hasura_tooling.util_postgres_query import run_postgres_query
from hasura_tooling.util_filepath_and_fileloader import (
    relationships_metadata_filepath,
    relationships_metadata,
)
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def get_all_rel_defs_from_pg():
    return run_postgres_query(
        """
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
                    table_schema = 'public' ) as t
                ORDER BY
                t.origin_table,
                t.remote_table,
                t.rel_name;
    """
    )


@click.command()
def export_metadata():
    """
    exports all relationships in flat format (no group by -- ex: by origin_table)
    to /servers/graphql2/tooling/metadata_relationships.yaml.

    Establishes/refreshes the relationships source of truth, which can be manipulated by the
    list_relationships_by() functionality to list relationships grouped-by origin or remote table.
    """
    all_rel_defs = get_all_rel_defs_from_pg()
    all_rels = []
    for rel_def in all_rel_defs:
        all_rels.append(
            {
                "origin_table": rel_def["origin_table"],
                "remote_table": rel_def["remote_table"],
                "rel_name": rel_def["rel_name"],
                "origin_column": rel_def["origin_column"],
                "remote_column": rel_def["remote_column"],
            }
        )
    with open(relationships_metadata_filepath(), "w") as f:
        yaml.dump(
            all_rels,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


@click.command()
@click.option(
    "-o", "--origin_table", "group_by_table", flag_value="origin_table", type=str
)
@click.option(
    "-r", "--remote_table", "group_by_table", flag_value="remote_table", type=str
)
@click.option("-f", "--filter", "filter", type=str, default="")
def list_relationships_by(group_by_table: str, filter: str):
    """
    Using /servers/graphql2/tooling/metadata_relationships.yaml created from export_metadata(),
    this function takes in a group-by parameter to manipulate then prints the results.

    Args:
    1. group-by: [origin_table/remote_table]

    """
    all_rel_defs = relationships_metadata()
    rel_print_dict: Dict[str, dict] = {}
    if group_by_table == "origin_table":
        for rel_def in all_rel_defs:
            if rel_def["origin_table"] not in rel_print_dict.keys():
                rel_print_dict[rel_def["origin_table"]] = {}
            rel_print_dict[rel_def["origin_table"]][rel_def["rel_name"]] = {
                "remote_table": rel_def["remote_table"],
                "origin_column": rel_def["origin_column"],
                "remote_column": rel_def["remote_column"],
            }
    elif group_by_table == "remote_table":
        for rel_def in all_rel_defs:
            remote_table = rel_def["remote_table"]
            origin_table = rel_def["origin_table"]
            if remote_table not in rel_print_dict.keys():
                rel_print_dict[remote_table] = {}
            if origin_table not in rel_print_dict[remote_table].keys():
                rel_print_dict[remote_table][origin_table] = {}
            rel_print_dict[remote_table][origin_table][rel_def["rel_name"]] = {
                "origin_column": rel_def["origin_column"],
                "remote_column": rel_def["remote_column"],
            }
    if filter == "":
        pprint(rel_print_dict)
    else:
        pprint(rel_print_dict[filter])


@click.group()
def tool():
    pass


tool.add_command(export_metadata)
tool.add_command(list_relationships_by)
cli = click.CommandCollection(sources=[tool])
if __name__ == "__main__":
    cli()
