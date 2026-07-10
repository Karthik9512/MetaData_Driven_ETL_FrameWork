# Databricks notebook source
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronze_metadata_path = '/Volumes/etl_framework_metadata/generated/yml/bronze_metadata.yml'
silver_rule_path = '/Volumes/etl_framework_metadata/config/yml/silver_rules.yml.txt'

with open(bronze_metadata_path,'r') as file:
    bronze_metadata = yaml.safe_load(file)
    
with open(silver_rule_path,'r') as file:
    silver_rules = yaml.safe_load(file)


# COMMAND ----------

silver_metadata = {
    'catalog' : 'ETL_Framework_metadata',
    'schema' : 'silver',
    'datasets' : []
    }

# COMMAND ----------

for dataset in bronze_metadata["datasets"]:

    table = dataset["table"]
    bronze_table = dataset["path"]

    print(f"Processing {table}")

    # Read Bronze table
    df = spark.read.table(bronze_table)

    # Get transformation rules
    rule = next(
        (r for r in silver_rules["datasets"] if r["table"] == table),
        None
    )

    if rule is None:
        print(f"No rule found for {table}")
        continue

    # ================= Common Transformations =================

    common = rule.get("common_transformations", {})

    # Trim string columns
    if common.get("trim"):
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, trim(col(field.name)))

    # Remove duplicates
    if common.get("remove_duplicates"):
        df = df.dropDuplicates()

    # Remove nulls
    if common.get("remove_nulls"):
        df = df.na.drop()

    # ================= Specific Transformations =================

    transformations = rule.get("transformations", {})

    # Rename columns
    rename_columns = transformations.get("rename_columns", {})

    for old_name, new_name in rename_columns.items():
        df = df.withColumnRenamed(old_name, new_name)

    # Cast columns
    cast_columns = transformations.get("cast_columns", {})

    for column_name, data_type in cast_columns.items():
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                col(column_name).cast(data_type)
            )

    # Uppercase
    uppercase_columns = transformations.get("uppercase", [])

    for column_name in uppercase_columns:
        df = df.withColumn(
            column_name,
            upper(col(column_name))
        )

    # Lowercase
    lowercase_columns = transformations.get("lowercase", [])

    for column_name in lowercase_columns:
        df = df.withColumn(
            column_name,
            lower(col(column_name))
        )

    # Drop columns
    drop_columns = transformations.get("drop_columns", [])

    if drop_columns:
        df = df.drop(*drop_columns)

    # Add columns
    add_columns = transformations.get("add_columns", {})

    for column_name, expression in add_columns.items():
        df = df.withColumn(
            column_name,
            expr(expression)
        )

    # ================= Write Silver Table =================

    silver_table = f"ETL_FrameWork_Metadata.silver.{table}"
    df.write.mode("overwrite").format('Delta').saveAsTable(silver_table)
    print(f"Completed {table}")

    # ================= Update Metadata =================

    silver_metadata["datasets"].append(
        {
            "table": table,
            "path": silver_table
        }
    )

# COMMAND ----------

output_path = '/Volumes/etl_framework_metadata/generated/yml/silver_metadata.yml'
with open(output_path,'w') as file:
    yaml.dump(
        silver_metadata,
        file,
        sort_keys=False
        )
print('Silver MetaData Generated Successfully')