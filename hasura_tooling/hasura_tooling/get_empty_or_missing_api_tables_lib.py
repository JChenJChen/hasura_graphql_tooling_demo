from typing import Dict, List

from pprint import pprint
from hasura_tooling.util_postgres_query import (
    concatenate_tuples_dict_list_to_single_dict_row,
    run_postgres_query,
)
import psycopg2
import logging


def get_all_exposed_tables_in_api() -> List[str]:
    return concatenate_tuples_dict_list_to_single_dict_row(
        run_postgres_query(
            """
        SELECT DISTINCT
        table_name
        FROM
        hdb_catalog.hdb_permission
        ORDER BY
        table_name;
    """
        )
    )["table_name"]


# limit 9 bc one table always only has 9 rows
def query_postgres_table_limit_10(
    table_name: str, lowest_known_table_row_count: int
) -> List[Dict]:
    return run_postgres_query(
        f"""
        SELECT *
        FROM
        public.{table_name}
        LIMIT {lowest_known_table_row_count};
        """
    )


def restart_from_specific_table(start_table: str) -> List[str]:
    all_tables = get_all_exposed_tables_in_api()
    try:
        start_index = all_tables.index(start_table)
        return all_tables[start_index:]
    except ValueError:
        raise ValueError(
            f"{start_table} is not in the list of tables with Hasura permissions."
        )


def cross_check_permission_denied_tables(table_name: str) -> List[Dict]:
    # if `select grant_permissions()` somehow fails to grant sufficient DB permissions,
    # code will check information_schema.tables for if table or view exists in public schema.
    # NOTE: we often use table and view interchangeably, this will return both tables AND views in public schema.
    return run_postgres_query(
        """
        SELECT table_name
        FROM
        information_schema.tables
        WHERE
        table_name = '{table_name}'
        AND
        table_schema = 'public';
    """.format(
            table_name=table_name
        )
    )


def get_empty_or_missing_api_tables(start_table: str = None):
    # manually add tables with known issues to be skipped (ex: correctly empty)
    skip_list: List[str] = []
    # currently set to 9 bc table with lowest count always only has 9 rows.
    lowest_known_table_row_count = 9
    if start_table:
        check_tables_list = restart_from_specific_table(start_table)
    else:
        check_tables_list = get_all_exposed_tables_in_api()
    existing_empty_tables = []
    manually_check_tables = []
    table_exists_but_count_not_confirmed = []
    table_does_not_exist = []
    logging.info("Begin checking for empty/missing API postgres tables...")
    for table in check_tables_list:
        if table in skip_list:
            continue
        logging.info(f"Currently on: {table}")
        try:
            res = query_postgres_table_limit_10(table, lowest_known_table_row_count)
            if len(res) == 0:
                existing_empty_tables.append(table)
                logging.warning(f"\n\n$$$$ EMPTY TABLE $$$$: {table}\n\n")
            # it would be strange for a table to fall under this condition, would probably warrant investigation
            elif len(res) < 9 and len(res) > 0:
                manually_check_tables.append(table)
                logging.warning(f"\n\n$$$$ less than 10 rows...? $$$$: {table}\n\n")
        except psycopg2.ProgrammingError as e:
            e_str = str(e)
            if "does not exist" in e_str:
                if len(cross_check_permission_denied_tables(table)) == 0:
                    table_does_not_exist.append({table: e_str})
                    logging.warning(
                        "\n\n$$$$ PERMISSION DENIED, AND TABLE NOT FOUND IN INFO_SCHEMA.TABLES $$$$: {e_str}\n\n"
                    )
                elif len(cross_check_permission_denied_tables(table)) == 1:
                    table_exists_but_count_not_confirmed.append({table: e_str})
                    logging.warning(
                        f"\n\n$$$$ PERMISSION DENIED, AND TABLE FOUND BUT COUNT NOT CONFIRMED $$$$: {e_str}\n\n"
                    )
                # impossible case where >1 matches from info_schema.tables -- check manually
                else:
                    manually_check_tables.append(table)
    results_printer(
        existing_empty_tables,
        table_does_not_exist,
        table_exists_but_count_not_confirmed,
        manually_check_tables,
    )


def results_printer(
    existing_empty_tables: list,
    table_does_not_exist: list,
    table_exists_but_count_not_confirmed: list,
    manually_check_tables: list,
):
    if len(existing_empty_tables) > 0:
        logging.warning(
            f"\n\n#### {len(existing_empty_tables)} Existing Emtpy Tables #####"
        )
        pprint(existing_empty_tables)
    if len(table_does_not_exist) > 0:
        logging.warning(
            f"\n\n#### {len(table_does_not_exist)} Table Does Not Exist #####"
        )
        pprint(table_does_not_exist)
    if len(table_exists_but_count_not_confirmed) > 0:
        logging.warning(
            f"\n\n#### {len(table_exists_but_count_not_confirmed)} Permission Denied Tables, check manually #####\n\n"
        )
        pprint(table_exists_but_count_not_confirmed)
    if len(manually_check_tables) > 0:
        logging.warning(
            "\n\n#### Check manually: Tables with count(*) < 10 =OR= multiple matches in info_schema.tables #####\n\n"
        )
        pprint(manually_check_tables)
    if len(existing_empty_tables) == 0 and len(table_does_not_exist) == 0:
        print("\n\n#### All Tables Present and Populated #####\n\n")
    tables_with_issues = {
        "table_does_not_exist": table_does_not_exist,
        "existing_empty_tables": existing_empty_tables,
        "manually_check_tables": manually_check_tables,
        "table_exists_but_count_not_confirmed": table_exists_but_count_not_confirmed,
    }
    return tables_with_issues
