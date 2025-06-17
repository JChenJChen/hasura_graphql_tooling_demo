import yaml
import os
from hasura_tooling.util_yaml_dumper import IndentedListYamlDumper


def scan_and_delete_duplicate_entry(metadata):
    for k, v in metadata.items():
        for i, j in v.items():
            temp_list = []
            for a in j:
                try:
                    assert a not in temp_list
                except AssertionError:
                    print("duplicate entry:" + str(a))
                    print("deleting duplicate entry" + str(a))
                    # after duplicate entry is identified, it will be deleted
                    metadata[k][i].remove(a)
                # if entry is not duplicate, add it to temp_list
                # for dupe-checking
                temp_list.append(a)


def main():
    # get path to hasura_perm_metadata.yaml
    graphql_parent_directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    features_directory = graphql_parent_directory + "/features"
    metadata_path = features_directory + "hasura_perm_metadata.yaml"
    # open hasura_perm_metadata
    with open(metadata_path) as f:
        metadata = yaml.safe_load(f)
    # run scan-and-delete-duplicate-entry function on metadata
    scan_and_delete_duplicate_entry(metadata)
    # commit changes to the file
    with open("../features/hasura_perm_metadata.yaml", "w") as f:
        yaml.dump(
            metadata,
            f,
            Dumper=IndentedListYamlDumper,
            default_flow_style=False,
            sort_keys=False,
        )


if __name__ == "__main__":
    main()
