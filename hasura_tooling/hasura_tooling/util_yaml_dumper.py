import textwrap

import yaml
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import LiteralScalarString


class IndentedListYamlDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(IndentedListYamlDumper, self).increase_indent(flow, False)


def string_representer(dumper, value):
    if value.isnumeric() or value == "":
        return dumper.represent_scalar(TAG_STR, value, style='"')
    return dumper.represent_scalar(TAG_STR, value)


TAG_STR = "tag:yaml.org,2002:str"
IndentedListYamlDumper.add_representer(str, string_representer)


def remove_leading_spaces(data):
    """
    Remove two leading spaces from lines of text.
    This is useful when dumping sequences using the ruamel yaml
    library.
    """
    results = []
    for line in data.splitlines(True):
        results.append(line[2:])
    return "".join(results)


def dump_remote_schema_metadata_to_yaml_file(metadata, yaml_file):
    """
    Dump formatted data using the ruamel yaml library. This is used
    so multi-line strings are formatted nicely, without newline characters.
    """
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.dump(metadata, yaml_file, transform=remove_leading_spaces)


def create_literal_scalar_string(s):
    """
    Create an object used by the ruamel yaml library for betting handling
    of multi-line strings.
    """
    return LiteralScalarString(textwrap.dedent(s))
