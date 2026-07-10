# Databricks notebook source
import yaml

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists ETL_FrameWork_Metadata;
# MAGIC create schema if not exists ETL_FrameWork_Metadata.config;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ETL_FrameWork_Metadata.bronze;
# MAGIC create schema if not exists ETL_FrameWork_Metadata.silver;
# MAGIC create schema if not exists ETL_FrameWork_Metadata.gold;
# MAGIC create schema if not exists ETL_FrameWork_Metadata.generated;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ETL_FrameWork_Metadata.source_data;

# COMMAND ----------


config_path = "/Volumes/etl_framework_metadata/config/yml/bronze01.yml.txt"

with open(config_path, "r") as file:
    config = yaml.safe_load(file)

display(config)

# COMMAND ----------

for dataset in config['datasets']:
    print(dataset['name'])

# COMMAND ----------

for dataset in config["datasets"]:

    source_path = config["source_path"]
    source_file = f"{source_path}/{dataset['source_file']}"

    target_table = f"ETL_FrameWork_Metadata.bronze.{dataset['target_table']}"

    try:
        df = (
            spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(source_file)
        )

        df.write \
          .mode("overwrite") \
          .format("delta") \
          .saveAsTable(target_table)

        print(f"✅ Loaded: {target_table}")

    except Exception as e:
        print(f"❌ Failed to load {source_file}")
        print(e)

# COMMAND ----------

bronze_metadata = {
    "catalog": "ETL_FrameWork_Metadata",
    "schema": "bronze",
    "datasets": []
}

for dataset in config["datasets"]:
    target_table = f"ETL_FrameWork_Metadata.bronze.{dataset['target_table']}"
    bronze_metadata["datasets"].append({
        "table": dataset["target_table"],
        "path": target_table
    })

# COMMAND ----------

output_path = "/Volumes/etl_framework_metadata/generated/yml/bronze_metadata.yml"

with open(output_path, "w") as file:
    yaml.dump(bronze_metadata, file, sort_keys=False)

print("Bronze metadata generated successfully.")