import os
import yaml
import logging
import click

from hasura_tooling.util_filepath_and_fileloader import sharded_tables_dir
from hasura_tooling.util_introspection import is_role_active
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


@click.command()
@click.argument("role_name")
@click.option("-t", "--to_true", type=bool, required=True)
def update_allow_agg(role_name: str, to_true: bool):
    # check if role exists in metadata_roles.yaml and is active.
    # If role's inactive, role shouldn't have any shards (to update by this tool).
    if is_role_active(role_name):
        shards_dir = sharded_tables_dir()
        target_file_name = f"{role_name}.yaml"
        for subdir, dirs, files in os.walk(shards_dir):
            if target_file_name in files:
                target_filepath = os.path.join(subdir, target_file_name)
                with open(target_filepath, "r") as p:
                    target_shard_contents = yaml.safe_load(p)
                if (
                    target_shard_contents["permission"]["allow_aggregations"]
                    is not to_true
                ):
                    with open(target_filepath, "w") as f:
                        target_shard_contents["permission"][
                            "allow_aggregations"
                        ] = to_true
                        yaml.dump(
                            target_shard_contents,
                            f,
                            Dumper=IndentedListYamlDumper,
                            default_flow_style=False,
                            sort_keys=False,
                        )
                        logging.info(f"Updated allow_agg to TRUE in {target_filepath}")
                    f.close()


@click.group()
def tool():
    pass


tool.add_command(update_allow_agg)
cli = click.CommandCollection(sources=[tool])
if __name__ == "__main__":
    logging.getLogger().setLevel(os.environ.get("LOGLEVEL", "INFO"))
    logging.info("Tooling Maturity Level: Alpha")
    cli()
