# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit
from delta.tables import DeltaTable
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Run from delta table to delta table.**

# CELL ********************

# Source metadata
source_workspace_id = "26f84b3b-c936-4482-b883-db691ee83597"
source_lakehouse_id = "66c3ae0f-6dc9-4028-9d4d-62afab6cc7e0" #bronze lakehouse
source_type = "lakehouse"  # or files/warehouse
source_storage_type = "Tables"
source_subfolder = "dbo"
source_name = "Drivers"
source_keys = "date,sku,brand,segment,category,channel,region,pack_type"

# Target metadata 
target_workspace_id = "26f84b3b-c936-4482-b883-db691ee83597"
target_lakehouse_id = "307568c6-a5f2-4bd1-9b58-34f495d97fe8" #silver lakehouse
target_type = "lakehouse"  # or "warehouse"
target_storage_type = "Tables"
target_schema = "Silver"
target_name = "Drivers"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = f"abfss://{source_lakehouse_id}@onelake.dfs.fabric.microsoft.com/{source_workspace_id}/{source_storage_type}/{source_subfolder}/{source_name}"
target_path = f"abfss://{target_lakehouse_id}@onelake.dfs.fabric.microsoft.com/{target_workspace_id}/{target_storage_type}/{target_schema}/{target_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(source_path)
display(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keys = [key.strip() for key in source_keys.split(",")]
merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in keys])

display(merge_condition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Detect file type
file_ext = os.path.splitext(source_path)[1].lower()

# Read logic based on file type
if file_ext == ".parquet":
    try:
        df = spark.read.parquet(source_path)
    except Exception as e:
        print("Parquet read failed due to unsupported types. Attempting workaround...")

        # Try reading schema only to identify safe columns
        schema_only = spark.read.parquet(source_path).schema
        safe_columns = [field.name for field in schema_only.fields if not isinstance(field.dataType, TimestampType)]

        # Read only safe columns
        df = spark.read.parquet(source_path).select(*safe_columns)

elif file_ext == ".csv":
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

else:
    raise ValueError(f"Unsupported file type: {file_ext}")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enrich with metadata
df_enriched = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256)) \
                .withColumn("loadtime", current_timestamp()) \
                .withColumn("source", lit(os.path.basename(source_path)))

display(df_enriched)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path_exists = mssparkutils.fs.exists(target_path)

print(f"Path exists: {path_exists}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not path_exists:
    df.write.mode("overwrite").format("delta").save(target_path)
    print("Data written in overwrite mode.")
else:
    delta_table = DeltaTable.forPath(spark, target_path)

    delta_table.alias("target").merge(
        df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("Data merged into existing table.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
