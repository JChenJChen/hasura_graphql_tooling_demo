import logging
from os import environ
import click

from hasura_tooling.get_empty_or_missing_api_tables_lib import (
    get_empty_or_missing_api_tables,
)


@click.command()
@click.option("-t", "--start_table", type=str, default=None, show_default=True)
def check_api_pg_tables(start_table: str = None):
    """
    Overview: Tool that gets list of public views with Hasura permissions, and queries them to confirm that they're
    not missing or empty.
    """
    get_empty_or_missing_api_tables(start_table)


@click.group()
def tool():
    pass


if __name__ == "__main__":
    tool.add_command(check_api_pg_tables)
    cli = click.CommandCollection(sources=[tool])
    logging.getLogger().setLevel(environ.get("LOGLEVEL", "INFO"))
    logging.info("Tooling Maturity Level: Beta")
    cli()
