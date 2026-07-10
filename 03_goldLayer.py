# Databricks notebook source
import yaml
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

silver_metadata_path = "/Volumes/etl_framework_metadata/generated/yml/silver_metadata.yml"
gold_rules_path = "/Volumes/etl_framework_metadata/config/yml/gold_rules.yml.txt"

with open(silver_metadata_path,"r") as f:
    silver_metadata = yaml.safe_load(f)

with open(gold_rules_path,"r") as f:
    gold_rules = yaml.safe_load(f)


# COMMAND ----------

for dataset in gold_rules["datasets"]:

    print(f"\nProcessing {dataset['table']}")

    dfs = {}

    # ---------------------------------------
    # Read Silver Tables
    # ---------------------------------------

    for table_name in dataset["source_tables"]:

        table_path = next(
            (
                t["path"]
                for t in silver_metadata["datasets"]
                if t["table"] == table_name
            ),
            None,
        )

        if table_path is None:
            raise Exception(f"{table_name} not found in silver_metadata.yml")

        dfs[table_name] = spark.read.table(table_path)

        print(f"Loaded {table_name}")

    # ---------------------------------------
    # Create Dimension Tables
    # ---------------------------------------

    for dim in dataset["dimensions"]:

        dim_df = dfs[dim["source"]]

        target_table = (
            f"{gold_rules['catalog']}."
            f"{gold_rules['schema']}."
            f"{dim['table']}"
        )

        dim_df.write.mode("overwrite").saveAsTable(target_table)

        print(f"Created {target_table}")

    # ---------------------------------------
    # Create Fact Table
    # ---------------------------------------

    fact_df = dfs["sales"].alias("sales")

    for join in dataset["joins"]:

        left_table, left_column = join["left"].split(".")
        right_table, right_column = join["right"].split(".")

        right_df = dfs[right_table].alias(right_table)

        fact_df = fact_df.join(
            right_df,
            col(f"{left_table}.{left_column}")
            == col(f"{right_table}.{right_column}"),
            join["type"],
        )

    # ---------------------------------------
    # Remove Duplicate Join Columns
    # ---------------------------------------

    fact_df = fact_df.select(
        col("sales.sales_id"),
        col("sales.customer_id"),
        col("sales.product_id"),
        col("sales.order_date"),
        col("sales.quantity"),
        col("sales.amount"),
        col("customers.customer_name"),
        col("products.product_name"),
        col("products.category"),
    )

    target_fact = (
        f"{gold_rules['catalog']}."
        f"{gold_rules['schema']}."
        f"{dataset['table']}"
    )

    fact_df.write.mode("overwrite").saveAsTable(target_fact)

    print(f"Created {target_fact}")

# ---------------------------------------
# Generate Gold Metadata
# ---------------------------------------

gold_metadata = {
    "catalog": gold_rules["catalog"],
    "schema": gold_rules["schema"],
    "datasets": [],
}

for dataset in gold_rules["datasets"]:

    gold_metadata["datasets"].append(
        {
            "table": dataset["table"],
            "path": f"{gold_rules['catalog']}.{gold_rules['schema']}.{dataset['table']}",
        }
    )

    for dim in dataset["dimensions"]:

        gold_metadata["datasets"].append(
            {
                "table": dim["table"],
                "path": f"{gold_rules['catalog']}.{gold_rules['schema']}.{dim['table']}",
            }
        )

# COMMAND ----------

output_path = "/Volumes/etl_framework_metadata/generated/yml/gold_metadata.yml"

with open(output_path, "w") as file:
    yaml.dump(gold_metadata, file, sort_keys=False)

print("\nGold Metadata Generated Successfully")