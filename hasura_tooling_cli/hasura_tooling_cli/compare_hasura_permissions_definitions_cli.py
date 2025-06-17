import click
import os
import logging
from pprint import pprint

from hasura_tooling.util_filepath_and_fileloader import (
    api_data_supersets_metadata,
    roles_metadata as roles_metadata_import,
    role_perm_vs_prescription_diff_dir,
    superset1_vs_superset2_diff_dir,
    role1_perm_vs_role2_perm_diff_dir,
)
from hasura_tooling.util_introspection import (
    check_api_data_superset_keys_exist,
)
from hasura_tooling.compare_hasura_permissions_definitions_lib import (
    get_role_permdef_shards,
    diff_2_perm_def_dicts,
    get_print_dict_case_keys,
)
from hasura_tooling.update_fn_create_generic_permissions_by_data_supersets import (
    get_api_tables_combined_dictionary,
)


# future considerations/TODO:
# - output to stdout by default
# - add -o/--output option to specify format (consider adding .json & .yaml for queryability and use in other scripts)
# - add warning in printout if role is inactive in metadata_roles.yaml
# - use *args instead to support variable # of inputs for compare_role_perm_def_to_prescription()


@click.command()
@click.argument("roles_input")
def compare_role_perm_def_to_prescription(roles_input: str):
    """For each role provided, compares each roles existing permissions definition to its prescription as described in
    metadata_roles.yaml and metadata_api_data_supersets.yaml.

    Args:
        roles (str): slash-delimited list of role_name's to compare, as seen in metadata_roles.yaml.

    Sample command:
        compare_role_perm_def_to_prescription role1/role2
    """
    roles = roles_input.lower().split("/")
    roles_metadata = roles_metadata_import()
    # "case" refers to which functionality of this diff tool
    case = "role_perm_vs_prescription"  # get_print_dict_case_keys value = ['role_actual_perms', 'prescription']
    for role in roles:
        try:
            role_supersets_prescription = roles_metadata[role]["api_data_supersets"]
        except KeyError:
            logging.warning(
                f"{role} does not exist in metadata_roles.yaml. Skipping..."
            )
            continue
        if check_api_data_superset_keys_exist(role_supersets_prescription):
            prescription_permdef = get_api_tables_combined_dictionary(
                role_supersets_prescription
            )
            role_actual_permdef = get_role_permdef_shards(role)
            diff_output_dir = role_perm_vs_prescription_diff_dir()
            if not os.path.exists(diff_output_dir):
                os.makedirs(diff_output_dir)
            diff_output_filepath = os.path.join(diff_output_dir, role) + ".txt"
            with open(diff_output_filepath, "w") as out:
                out.write(f"\nROLE-VS-PRESCRIPTION for {role}\n")
                consolidated_print_dict = diff_2_perm_def_dicts(
                    role_actual_permdef, prescription_permdef, case
                )
                if (
                    len(
                        consolidated_print_dict[
                            f"only_in_{get_print_dict_case_keys(case)[0]}"
                        ]
                    )
                    > 0
                ):
                    out.write("\n###### ONLY IN ROLE ACTUAL PERMISSIONS #######\n")
                    pprint(
                        consolidated_print_dict[
                            f"only_in_{get_print_dict_case_keys(case)[0]}"
                        ],
                        stream=out,
                    )
                else:
                    out.write(
                        "\n###### ALL ACTUAL TABLE PERMISSIONS PRESENT IN PERMISSIONS PRESCRIPTION #######\n"
                    )
                if (
                    len(
                        consolidated_print_dict[
                            f"only_in_{get_print_dict_case_keys(case)[1]}"
                        ]
                    )
                    > 0
                ):
                    out.write(
                        "\n###### PERMISSION PRESCRIPTION MISSING IN ROLE ACTUAL PERMISSIONS #######\n"
                    )
                    pprint(
                        consolidated_print_dict[
                            f"only_in_{get_print_dict_case_keys(case)[1]}"
                        ],
                        stream=out,
                    )
                else:
                    out.write(
                        "\n###### ALL TABLE PERMISSIONS IN PRESCRIPTION PRESENT IN ROLE ACTUAL PERMISSIONS #######\n"
                    )
                if len(consolidated_print_dict["in_both_diff"]) > 0:
                    out.write(
                        "\n###### TABLE PERMISSIONS THAT EXISTS IN BOTH BUT DIFFER IN DEFINTION #######\n"
                    )
                    pprint(consolidated_print_dict["in_both_diff"], stream=out)
            print(
                f"Permissions-vs-prescription diffs for {role} output to {diff_output_filepath}."
            )


@click.command()
@click.argument("superset1")
@click.argument("superset2")
def compare_two_superset_perm_defs(superset1: str, superset2: str):
    """Compares 2 supersets (from metadata_api_data_supersets.yaml).

    Args:
        supersets (str): slash-delimited list of 2 superset names to compare,
        as seen in metadata_api_data_supersets.yaml.

    Sample command:
        compare_two_superset_perm_defs superset1 superset2
    """
    case = "superset1_vs_superset2"  # get_print_dict_case_keys value = ['superset_1', 'superset_2']
    if check_api_data_superset_keys_exist([superset1, superset2]):
        supersets_metadata = api_data_supersets_metadata()
        superset1_perm_def = supersets_metadata[superset1]
        superset2_perm_def = supersets_metadata[superset2]
        diff_output_dir = superset1_vs_superset2_diff_dir()
        if not os.path.exists(diff_output_dir):
            os.makedirs(diff_output_dir)
        diff_output_filepath = (
            os.path.join(diff_output_dir, f"{superset1}-vs-{superset2}") + ".txt"
        )
        with open(diff_output_filepath, "w") as out:
            out.write(f"SUPERSET1: {superset1} =VS= SUPERSET2: {superset2}\n")
            consolidated_print_dict = diff_2_perm_def_dicts(
                superset1_perm_def, superset2_perm_def, case
            )
            if (
                len(
                    consolidated_print_dict[
                        f"only_in_{get_print_dict_case_keys(case)[0]}"
                    ]
                )
                > 0
            ):
                out.write(f"\n###### ONLY IN SUPERSET1 ({superset1}) #######\n")
                pprint(
                    consolidated_print_dict[
                        f"only_in_{get_print_dict_case_keys(case)[0]}"
                    ],
                    stream=out,
                )
            else:
                out.write(
                    f"\n##### ALL TABLES PERMS IN SUPERSET1 ({superset1}) PRESENT IN SUPERSET2 ({superset2}) #######\n"
                )
            if (
                len(
                    consolidated_print_dict[
                        f"only_in_{get_print_dict_case_keys(case)[1]}"
                    ]
                )
                > 0
            ):
                out.write(f"\n###### ONLY IN SUPERSET2 ({superset2}) #######\n")
                pprint(
                    consolidated_print_dict[
                        f"only_in_{get_print_dict_case_keys(case)[1]}"
                    ],
                    stream=out,
                )
            else:
                out.write(
                    f"\n###### ALL TABLE PERMS IN SUPERSET2 ({superset2}) PRESENT IN SUPERSET1 ({superset1}) #######\n"
                )
            if len(consolidated_print_dict["in_both_diff"]) > 0:
                out.write(
                    "\n###### TABLE PERMISSIONS THAT EXISTS IN BOTH BUT DIFFER IN DEFINTION #######\n"
                )
                pprint(consolidated_print_dict["in_both_diff"], stream=out)
        print(
            f"Superset-vs-superset diffs for `{superset1}` and `{superset2}` output to {diff_output_filepath}"
        )


@click.command()
@click.argument("role1")
@click.argument("role2")
def compare_two_role_perm_defs(role1: str, role2: str):
    """Compares two roles' existing permissions shards.

    Args:
        role1 (str): first role_name to compare, as seen in metadata_roles.yaml.
        role2 (str): second role_name to compare, as seen in metadata_roles.yaml.

    Sample command:
        compare_two_role_perm_defs role1 role2
    """
    case = "role1_perm_vs_role2_perm"  # get_print_dict_case_keys value = ['role_1', 'role_2']
    role1_actual_permdef = get_role_permdef_shards(role1)
    role2_actual_permdef = get_role_permdef_shards(role2)
    diff_output_dir = role1_perm_vs_role2_perm_diff_dir()
    if not os.path.exists(diff_output_dir):
        os.makedirs(diff_output_dir)
    diff_output_filepath = os.path.join(diff_output_dir, f"{role1}-vs-{role2}") + ".txt"
    with open(diff_output_filepath, "w") as out:
        out.write(f"ROLE1: {role1} =VS= ROLE2: {role2}\n")
        consolidated_print_dict = diff_2_perm_def_dicts(
            role1_actual_permdef, role2_actual_permdef, case
        )
        if (
            len(consolidated_print_dict[f"only_in_{get_print_dict_case_keys(case)[0]}"])
            > 0
        ):
            out.write(f"\n###### ONLY IN ROLE1 ({role1}) PERMISSIONS #######\n")
            pprint(
                consolidated_print_dict[f"only_in_{get_print_dict_case_keys(case)[0]}"],
                stream=out,
            )
        else:
            out.write(
                f"\n###### ALL ROLE1 ({role1}) PERMISSIONS PRESENT IN ROLE2 ({role2}) PERMISSIONS #######\n"
            )
        if (
            len(consolidated_print_dict[f"only_in_{get_print_dict_case_keys(case)[1]}"])
            > 0
        ):
            out.write(f"\n###### ONLY IN ROLE2 ({role2}) PERMISSIONS #######\n")
            pprint(
                consolidated_print_dict[f"only_in_{get_print_dict_case_keys(case)[1]}"],
                stream=out,
            )
        else:
            out.write(
                f"\n###### ALL ROLE2 ({role2}) PERMISSIONS PRESENT IN ROLE1 ({role1}) PERMISSIONS #######\n"
            )
        if len(consolidated_print_dict["in_both_diff"]) > 0:
            out.write(
                "\n###### TABLE PERMISSIONS THAT EXISTS IN BOTH BUT DIFFER IN DEFINTION #######\n"
            )
            pprint(consolidated_print_dict["in_both_diff"], stream=out)
    print(
        f"{role1}-vs-{role2} Roles Permissions Diffs for output to {diff_output_filepath}."
    )


@click.group()
def tool():
    pass


tool.add_command(compare_role_perm_def_to_prescription)
tool.add_command(compare_two_superset_perm_defs)
tool.add_command(compare_two_role_perm_defs)
cli = click.CommandCollection(sources=[tool])
if __name__ == "__main__":
    """
    For syntax help:
    - python  --help
    - Please note that Click replaces all function underscores with dashes (reflected in CLI help menu)
    """
    print("Tooling Maturity Level: Alpha")
    cli()
