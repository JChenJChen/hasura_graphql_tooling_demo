import os
from shutil import copyfile

# assumes running from ~/cache dir

shards_rootdir = "/graphql/metadata/tables"
for table_subdir in os.listdir(shards_rootdir):
    full_table_subdir_path = os.path.join(shards_rootdir, table_subdir)
    print(full_table_subdir_path)
    if os.path.isdir(full_table_subdir_path):
        for f in os.listdir(full_table_subdir_path):
            full_file_path = os.path.join(full_table_subdir_path, f)
            print(full_file_path)
            if os.path.isfile(full_file_path) and f == 'role_name.yaml':
                copyfile(full_file_path, f"/cache/role_name_{table_subdir}.yaml")
