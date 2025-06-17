import pytest

from hasura_tooling.update_fn_create_generic_permissions_by_data_supersets import (
    merge_table_permission_definitions,
)


def sample_superset1():
    return {
        "allow_aggregations": True,
        "columns": ["1", "2", "2"],
        "computed_fields": [],
        "filter": {},
        "limit": 1111,
    }


def sample_superset2():
    return {
        "allow_aggregations": False,
        "columns": ["2", "2a"],
        "computed_fields": ["computed_field_2"],
        "filter": {"filter_2": "something2"},
        "limit": 222,
    }


@pytest.fixture(scope="session")
def merge_1_and_2():
    return merge_table_permission_definitions(sample_superset1(), sample_superset2())


def test_permissions_merge_fn_allow_agg(merge_1_and_2: dict):
    assert merge_1_and_2["allow_aggregations"] is True


def test_permissions_merge_fn_columns(merge_1_and_2: dict):
    # tests that set of union of columns is the result -- no duplicates.
    merge_1_and_2["columns"].sort()
    assert merge_1_and_2["columns"] == ["1", "2", "2a"]


def test_permissions_merge_fn_filter(merge_1_and_2: dict):
    assert merge_1_and_2["filter"] == {"filter_2": "something2"}


def test_permissions_merge_fn_computed_fields(merge_1_and_2: dict):
    assert merge_1_and_2["computed_fields"] == ["computed_field_2"]


def test_permissions_merge_fn_limit(merge_1_and_2: dict):
    assert merge_1_and_2["limit"] == 1111
