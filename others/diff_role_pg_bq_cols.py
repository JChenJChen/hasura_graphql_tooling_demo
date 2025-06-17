import yaml
import os

pg_perm_shards_dir = "/temp/role_pg_api_perms"
bq_perm_shards_dir = "/graphql2/role/metadata/databases/display-role/tables"

pg_shards_files = set(os.listdir(pg_perm_shards_dir))
bq_shards_files = set(os.listdir(bq_perm_shards_dir))
bq_shards_files.remove('tables.yaml')
pg_not_in_bq = list(pg_shards_files - bq_shards_files)
bq_not_in_pg = list(bq_shards_files - pg_shards_files)
print("PG TABLE PERMS MISSING IN BQ")
print(pg_not_in_bq)
print("\nBQ TABLE PERMS MISSING IN PG")
print(bq_not_in_pg)
in_both = list(pg_shards_files & bq_shards_files)
in_both.sort()
print("\n permissions in both BQ & PG:")
for f in in_both:
    pg_f_fullpath = os.path.join(pg_perm_shards_dir, f)
    bq_f_fullpath = os.path.join(bq_perm_shards_dir, f)
    with open(pg_f_fullpath, "r") as p:
        pg_shard_contents = yaml.safe_load(p)
    pg_columns = set(pg_shard_contents['permission']['columns'])
    with open(bq_f_fullpath, "r") as q:
        bq_shard_contents = yaml.safe_load(q)
    bq_columns = set(bq_shard_contents['select_permissions'][0]['permission']['columns'])
    pg_columns_not_in_bq = pg_columns - bq_columns
    bq_columns_not_in_pg = bq_columns - pg_columns
    # exit()
    if len(bq_columns_not_in_pg) > 0 or len(pg_columns_not_in_bq) > 0:
        print(f"\ntable_name: {f}")
        if len(bq_columns_not_in_pg) > 0:
            print("pg_columns_not_in_bq:")
            print(bq_columns_not_in_pg)
            # print(len(bq_columns_not_in_pg))
        if len(pg_columns_not_in_bq) > 0:
            print("pg_columns_not_in_bq:")
            print(pg_columns_not_in_bq)
            # print(len(pg_columns_not_in_bq))
