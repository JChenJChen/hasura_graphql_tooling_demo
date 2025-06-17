import click
import logging
from os import environ

# import API metadata update tooling
from hasura_tooling.update_fn_create_generic_permissions_by_data_supersets import (
    sync_permission_shards_by_roles,
)
from hasura_tooling.update_fn_create_relationships import orchestrator
from hasura_tooling.update_fn_delete_all_permissions_by_roles import (
    delete_permission_shards,
)
from hasura_tooling.util_introspection import is_role_active, introspect_role_supersets
from hasura_tooling.shard_hasura_tables_yaml_lib import (
    reconstruct_sharded_hasura_tables_yaml,
    refresh_tables_yaml_shards,
)
from hasura_tooling.create_bq_metadata_by_role import create_bq_api_metadata_by_role
from hasura_tooling.remote_schema_permissions import (
    add_remote_schema_permissions,
    remove_remote_schema_permissions,
)


@click.command()
@click.argument("roles")
def sync_roles(roles: str):
    """
    Updates Hasura metadata according to source-of-truth yamls by roles.

    **SETUP STEPS**

    1. Confirm the roles' api_data_supersets assignment in metadata_roles.yaml,
    and is_active setting are both correct.

    2. Confirm the roles' supersets' permissions definition attributes (including the columns, allow_agg, filters, etc)
    are 100% accurate in metadata_api_data_supersets.yaml.

    Args:

    roles, delimited with '/'

    **NOTES ON ARGS**:
    - This functionality is a wrapper of update-permissions-by-supersets and delete-all-permissions-by-roles,
    and invokes either depending on is_active status of role.

    Example Command:
    python hasura_tooling_cli sync-roles role1/role2/role3
    """
    role_array = roles.split("/")
    for role in role_array:
        if is_role_active(role):
            logging.info(f"{role} is ACTIVE, syncing permissions...")
            sync_permission_shards_by_roles(role, introspect_role_supersets(role))
            add_remote_schema_permissions(role)
        else:
            logging.info(f"{role} is INACTIVE, deleting all permissions...")
            delete_permission_shards(role)
            remove_remote_schema_permissions(role)


@click.command()
def create_relationships():
    """
    Bulk creates relationship from a metadata .txt file (currently)
    Args:
        None, but reads in hasura_relationships_metadata.txt as the driving metadata file.
    """
    orchestrator()


@click.command()
@click.argument("roles")
def delete_roles(roles: str):
    """Deletes all permissions by roles.

    Args:

        roles (str): slash-delimited list of role_name's to delete table permissions for.

    Example Command:

    python hasura_tooling_cli delete-roles role1/role2/role3
    """
    delete_permission_shards(roles)
    for role in roles.split("/"):
        remove_remote_schema_permissions(role)


@click.command()
@click.option(
    "-o", "--overwrite", "overwrite", type=bool, required=True, show_default=True
)
def reconstruct(overwrite: bool):
    """
    Reconstructs shards into hasura metadata.

    reconstructs the Hasura metadata shards in /servers/graphql2/metadata/tables/
    into config v3 table-by-table metadata -- the Hasura metadata that is actually deployed.
    This function is called at the end of sync-roles fn to overwrite & refresh hasura metadata.

    NOTE:

    If you still wish to update Hasura metadata manually, you must refresh the shards manually first,
    and then reconstruct hasura metadata after completing all manual updates:

    1. Run `python shard_hasura_tables_yaml_cli.py shard`.

    2. Make all of your updates.

    3. Run `python shard_hasura_tables_yaml_cli.py reconstruct`

    **IMPORTANT**
    If merge conflicts arise, resolve merge conflicts within the shards first, then you MUST reconstruct hasura metadata

    Options:

    -o, --overwrite: whether to overwrite hasura metadata, defaults to True.
    """
    reconstruct_sharded_hasura_tables_yaml(overwrite)


@click.command()
@click.option(
    "-r",
    "--refresh",
    "refresh",
    type=bool,
    default=True,
    required=True,
    show_default=True,
)
def shard(refresh: bool):
    """
    Shards hasura metadata, which breaks down git diffs by file and makes it more reviewable in PRs.

    Additional Details: refreshes shards -- Deletes all shards and recreates them from hasura metadata.
    Invoked by hasura_tooling at the start of beginning of the workflow to ensure consistency.

    NOTE:

    If you still wish to update Hasura metadata manually, you must refresh the shards manually first,
    and then reconstruct hasura metadata after completing all manual updates:

    1. Run `python shard_hasura_tables_yaml_cli.py shard`.

    2. Make all of your updates.

    3. Run `python shard_hasura_tables_yaml_cli.py reconstruct`

    **IMPORTANT**
    If merge conflicts arise, resolve merge conflicts within the shards THEN reconstruct hasura metadata.
    Resolving conflicts within hasura metadata is unnecessary.

    Options:

    -r, --refresh: whether to delete all contents of shards directory (`servers/graphql2/metadata/tables/),
    or to just only shard hasura metadata and update shards. DEFAULTS TO TRUE.

    The risk of not refreshing: if there are extraneous shards those shards will remain and cause regressions.
    """
    refresh_tables_yaml_shards(refresh)


@click.command()
@click.argument("role")
@click.argument("bq_project")
@click.option(
    "-o", "--overwrite", "overwrite", type=bool, required=False, show_default=True
)
def create_bq_metadata(role: str, bq_project: str, overwrite: bool):
    """
    Create BigQuery API Metadata based on PG API Metadata by role

    Args:

        role (str): role to copy metadata from

        bq_project: BigQuery project to create BigQuery API Metadata for

        overwrite (bool): whether to overwrite BigQuery API Metadata


    Example Command:

    python hasura_tooling_cli create-bq-metadata role1 bq_project-1 --overwrite

    """
    create_bq_api_metadata_by_role(role, bq_project, overwrite)


@click.group()
def tool():
    pass


tool.add_command(create_relationships)
tool.add_command(sync_roles)
tool.add_command(shard)
tool.add_command(reconstruct)
tool.add_command(create_bq_metadata)
cli = click.CommandCollection(sources=[tool])
if __name__ == "__main__":
    """
    For syntax help:
    - python3 hasura_metadata_updater.py --help
    - Please note that Click replaces all function underscores with dashes (reflected in CLI help menu)
    """
    logging.getLogger().setLevel(environ.get("LOGLEVEL", "INFO"))
    logging.info("Tooling Maturity Level: Alpha")
    cli()
