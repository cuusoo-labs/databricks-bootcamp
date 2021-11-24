# Databricks notebook source
setup_responses = dbutils.notebook.run("../../includes/Setup-Batch-GDrive", 0).split()

dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
bronze_table_path = f"{dbfs_data_path}tables/bronze"
silver_table_path = f"{dbfs_data_path}tables/silver"
silver_clone_table_path = f"{dbfs_data_path}tables/silver_clone"
silver_sh_clone_table_path = f"{dbfs_data_path}tables/silver_clone_shallow"
silver_constraints_table_path = f"{dbfs_data_path}tables/silver_constraints"
gold_table_path = f"{dbfs_data_path}tables/gold"
gold_agg_table_path = f"{dbfs_data_path}tables/goldagg"
parquet_table_path = f"{dbfs_data_path}tables/parquet"
autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"
dbutils.fs.rm(bronze_table_path, recurse=True)
dbutils.fs.rm(silver_table_path, recurse=True)
dbutils.fs.rm(gold_table_path, recurse=True)
dbutils.fs.rm(parquet_table_path, recurse=True)
dbutils.fs.rm(silver_clone_table_path, recurse=True)

print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))
print("Bronze Table Location is {}".format(bronze_table_path))
print("Silver Table Location is {}".format(silver_table_path))
print("Gold Table Location is {}".format(gold_table_path))
print("Parquet Table Location is {}".format(parquet_table_path))
