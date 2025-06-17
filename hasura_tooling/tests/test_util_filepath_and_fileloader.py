from os import path, name
import logging

from hasura_tooling.util_filepath_and_fileloader import (
    sharded_tables_dir,
    metadata_dir,
    end_to_end_tests_root_dir,
    relationships_end_to_end_dir,
    migrations_dir,
)


def directory_is_valid(dir: str) -> bool:
    if name == "nt":
        if "/" in dir:
            logging.error(f"Forward slash in {dir}")
            return False
    else:
        if "\\" in dir:
            logging.error(f"Backslash in {dir}")
            return False
    return path.isdir(dir)


class TestUtilFilePathAndFileLoader:
    def test_sharded_tables_dir(self):
        res = sharded_tables_dir()

        assert res
        assert directory_is_valid(res)

    def test_metadata_dir(self):
        res = metadata_dir()

        assert res
        assert directory_is_valid(res)

        def test_end_to_end_tests_root_dir(self):
            res = end_to_end_tests_root_dir()

            assert res
            assert "feature" in res
            assert "features" in res
            assert directory_is_valid(res)

        def test_relationship_end_to_end(self):
            res = relationships_end_to_end_dir()

            assert res
            assert "relationships" in res
            assert directory_is_valid(res)

    def test_migrations_dir(self):
        res = migrations_dir()

        assert res
        assert "migrations" in res
        assert directory_is_valid(res)
