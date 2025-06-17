"""
To account for the differences between actual table names and their aliases,
this tooling takes in a table's actual name and returns the alias name.

This is useful bc the tooling has a (weak but foundational) assumption that the table names
represented in the tooling metadata and tooling is how they appear in the API, which is the
table alias name if one is present.
"""


def translate_actual_table_name_to_alias(table_name):
    alias_dict = table_name_alias_lookup()
    if table_name in alias_dict.keys():
        print(
            "Translated: {actual} -> {alias}".format(
                actual=table_name, alias=alias_dict[table_name]
            )
        )
        return alias_dict[table_name]
    else:
        return table_name


def table_name_alias_lookup():
    alias_table_names_dict = {
        "raw_table_name_1": "display_table_name_1",
        "raw_table_name_2": "display_table_name_2",
        "raw_table_name_3": "display_table_name_3",
    }
    return alias_table_names_dict
