import os
import psycopg2
from typing import List, Dict
import logging

"""
Helper functions that:
    1. get_common_postgres_envs: get postgres connection values from env vars.
    2. create_postgres_connection: creates postgres connection from env vars.
    3. run_postgres_query: create PG cursor, runs a passed-in query, fetches the results,
        and compiles the results into a Python dictionary.
    4. tuples_to_dicts: uses tuples_to_dict to compile query results into a Python dictionary.

"""


def get_common_postgres_envs() -> Dict:
    """Returns a common set of postgres envs.
    Returns:
        dict -- The postgres env set.
    """
    pg_username = (
        os.environ.get("PGUSER")
        if os.environ.get("PGUSERNAME") is None
        else os.environ.get("PGUSERNAME")
    )

    pg_common_envs = {
        "PGHOST": os.environ.get("PGHOST"),
        "PGPORT": os.environ.get("PGPORT"),
        "PGUSER": pg_username,
        "PGPASSWORD": os.environ.get("PGPASSWORD"),
        "PGDATABASE": os.environ.get("PGDATABASE"),
    }

    return pg_common_envs


def create_postgres_connection() -> psycopg2.extensions.connection:
    """Creates psycopg2 connection string using envs
    envs:
        PGHOST  URL of host postgres database
        PGDATABASE  database name of host postgres database
        PGPORT  connection port of host postgres database
        PGUSER  login username of connection string for host database connection
        PGPASSWORD  login password of connection string for host database connection
    Returns:
        connection -- The newly created connection
    """
    envs = get_common_postgres_envs()
    return psycopg2.connect(
        host=envs["PGHOST"],
        database=envs["PGDATABASE"],
        user=envs["PGUSER"],
        password=envs["PGPASSWORD"],
        port=envs["PGPORT"],
    )


def run_postgres_query(query: str) -> List[Dict]:
    try:
        connection = create_postgres_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        query_results = cursor.fetchall()
        results = tuples_to_dicts(query_results, cursor.description)
        cursor.close()
        connection.close()
        return results
    except psycopg2.OperationalError:
        logging.error(
            "\n\n##### HINT: ##### Did you `source envs` and check PGPORT is correct?\n"
        )
        return []


def tuple_to_dict(tpl: tuple, description: tuple) -> Dict:
    res: Dict[str, str] = {}
    for i, column in enumerate(description):
        res.setdefault(column.name, tpl[i])
    return res


def tuples_to_dicts(data: List[tuple], description: tuple) -> List[Dict]:
    return [tuple_to_dict(item, description) for item in data]


def concatenate_tuples_dict_list_to_single_dict_row(
    tuples_dict_list: List[Dict],
) -> Dict[str, List]:
    return_dict: Dict[str, list] = {}
    key = list(tuples_dict_list[0].keys())[0]
    return_dict[key] = []
    for row in tuples_dict_list:
        return_dict[key].append(row[key])
    return return_dict
