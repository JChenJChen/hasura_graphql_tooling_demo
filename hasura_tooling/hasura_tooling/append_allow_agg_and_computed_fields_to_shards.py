import yaml
import os

from hasura_tooling.util_filepath_and_fileloader import sharded_tables_dir
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper
from hasura_tooling.update_fn_create_generic_permissions_by_data_supersets import (
    permissions_block_template,
)


def append_allow_agg_and_computed_fields_to_shards():
    """
    PURPOSE: `hasura metadata export` truncates instances of allow_aggregations=False &
    computed_fields=[] bc it's deemed redundant, but we've opted for explicit declaration
    for all permissions_definition fields. Adding these fields will greatly reduce file diffs
    merely due to the omission of these 2 fields.
    """
    for subdir, dirs, files in os.walk(sharded_tables_dir()):
        for f in files:
            if f != "_table.yaml":
                with open(os.path.join(subdir, f), "r") as p:
                    shard_contents = yaml.safe_load(p)
                if "computed_fields" not in shard_contents["permission"].keys():
                    shard_contents["permission"]["computed_fields"] = []
                if "allow_aggregations" not in shard_contents["permission"].keys():
                    shard_contents["permission"]["allow_aggregations"] = False
                # if 'filter' not in shard_contents['permission'].keys():
                #     shard_contents['permission']['filter'] = {}
                formatted_output_shard = permissions_block_template(
                    shard_contents["role"],
                    shard_contents["permission"]["columns"],
                    shard_contents["permission"]["limit"],
                    shard_contents["permission"]["allow_aggregations"],
                    shard_contents["permission"]["computed_fields"],
                    shard_contents["permission"]["filter"],
                )
                with open(os.path.join(subdir, f), "w") as p:
                    yaml.dump(
                        formatted_output_shard,
                        p,
                        Dumper=IndentedListYamlDumper,
                        default_flow_style=False,
                        sort_keys=False,
                    )


if __name__ == "__main__":
    append_allow_agg_and_computed_fields_to_shards()
