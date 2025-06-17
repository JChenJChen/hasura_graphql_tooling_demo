import pytest
from hasura_tooling.get_empty_or_missing_api_tables_lib import (
    get_empty_or_missing_api_tables,
)


@pytest.fixture(scope="session")
def tables_with_issues():
    tables_with_issues = get_empty_or_missing_api_tables()
    return tables_with_issues


@pytest.mark.skip(reason="integration test that requires postgres DB connection.")
def test_api_pg_tables_not_missing(tables_with_issues):
    assert tables_with_issues["table_does_not_exist"] == []


@pytest.mark.skip(reason="integration test that requires postgres DB connection.")
def test_api_pg_tables_not_empty(tables_with_issues):
    assert tables_with_issues["existing_empty_tables"] == []


@pytest.mark.skip(reason="integration test that requires postgres DB connection.")
def test_api_pg_tables_unclear_if_good(tables_with_issues):
    assert tables_with_issues["manually_check_tables"] == []
    # table exists in info_schema.tables, but cannot be queried to confirm row count
    assert tables_with_issues["table_exists_but_count_not_confirmed"] == []
