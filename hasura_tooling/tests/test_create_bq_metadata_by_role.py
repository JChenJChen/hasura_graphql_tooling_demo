from os import path

from hasura_tooling import create_bq_metadata_by_role


class TestCreateBqMetadataByRole:
    def test_load_yaml_files(self):
        test_folder = path.join(path.dirname(path.abspath(__file__)), "test_metadata")
        res = create_bq_metadata_by_role.load_yaml_files("test_role", test_folder)

        assert res

    def test_build_relationships(self):
        test_folder = path.join(path.dirname(path.abspath(__file__)), "test_metadata")
        yaml_files = create_bq_metadata_by_role.load_yaml_files("test_role", test_folder)
        res = create_bq_metadata_by_role.build_relationships(
            "test_table", yaml_files, "object_relationships"
        )
        assert not res
        res = create_bq_metadata_by_role.build_relationships(
            "test_table", yaml_files, "array_relationships"
        )
        assert res
        assert len(res) == 1
        assert "schema" not in res[0]["using"]["manual_configuration"]["remote_table"]
        assert "dataset" in res[0]["using"]["manual_configuration"]["remote_table"]
        assert (
            res[0]["using"]["manual_configuration"]["remote_table"]["name"]
            == "test_table"
        )
        assert (
            res[0]["using"]["manual_configuration"]["remote_table"]["dataset"]
            == "display_#timestamp#"
        )

    def test_build_table_info(self):
        test_folder = path.join(path.dirname(path.abspath(__file__)), "test_metadata")
        yaml_files = create_bq_metadata_by_role.load_yaml_files("test_role", test_folder)
        table_yaml_file = yaml_files["test_table"]
        res = create_bq_metadata_by_role.build_table_info(
            "test_table", table_yaml_file
        )
        assert res["table"]["name"] == "test_table"
        assert res["table"]["dataset"] == "display_#timestamp#"
        assert res["configuration"]["custom_name"] == "custom_table_name"
        assert res["configuration"]["custom_column_names"] == {
            "last_seen_date": "last_seen"
        }

    def test_build_permissions(self):
        test_folder = path.join(path.dirname(path.abspath(__file__)), "test_metadata")
        yaml_files = create_bq_metadata_by_role.load_yaml_files("test_role", test_folder)
        res = create_bq_metadata_by_role.build_permissions(
            "test_role", yaml_files["test_table"]
        )

        assert res
        assert "address" in res[0]["permission"]["columns"]
        assert res[0]["role"] == "test_role"
