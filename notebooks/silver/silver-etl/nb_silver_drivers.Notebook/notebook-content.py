# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "66c3ae0f-6dc9-4028-9d4d-62afab6cc7e0",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "26f84b3b-c936-4482-b883-db691ee83597",
# META       "known_lakehouses": [
# META         {
# META           "id": "66c3ae0f-6dc9-4028-9d4d-62afab6cc7e0"
# META         },
# META         {
# META           "id": "307568c6-a5f2-4bd1-9b58-34f495d97fe8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils, notebook
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit, col
from pyspark.sql import functions as function
from delta.tables import DeltaTable
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#structural preferences: if we want to add a schema prefix to our table name (ie:True = bronze_tablename, False = tablename)
schema_parse = False #default to False

# Source metadata
source_workspace_id = "26f84b3b-c936-4482-b883-db691ee83597"
source_lakehouse_id = "66c3ae0f-6dc9-4028-9d4d-62afab6cc7e0" #bronze lakehouse
source_type = "lakehouse"  # or files/warehouse
source_storage_type = "Tables" #Files or Tables
source_subfolder = "taxi-raw"
source_schema = "bronze" #not an actual schema.. can act as one though through prefix if needed (use bronze, silver, gold)
source_name = "drivers"
source_keys = "LicenseNumber"

# Target metadata 
target_workspace_id = "26f84b3b-c936-4482-b883-db691ee83597"
target_lakehouse_id = "307568c6-a5f2-4bd1-9b58-34f495d97fe8" #silver lakehouse 307568c6-a5f2-4bd1-9b58-34f495d97fe8
target_type = "lakehouse"  # or "warehouse"
target_storage_type = "Tables"
target_schema = "silver" #not an actual schema.. can act as one though through prefix if needed (use bronze, silver, gold)
target_name = "drivers"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set dynamically
if source_storage_type == "Tables":
    if schema_parse is True:
        source_path = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/{source_storage_type}/{source_schema}_{source_name}"
    else:
        source_path = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/{source_storage_type}/{source_name}"


# set dynamically
if schema_parse is True:
    target_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/{target_storage_type}/{target_schema}_{target_name}"
else:
    target_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/{target_storage_type}/{target_name}"


# set if source is table, use bellow for df lkp
if source_storage_type == "Tables":
    delta_source = f"{source_storage_type}/{source_name}"
else:
    delta_source = "Empty" # overkill?



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(source_path)
display(target_path)
display(delta_source)



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
## new logic for going between delta tables
elif source_storage_type == "Tables":
    df = spark.read.format("delta").load(delta_source)
    print("Delta table source.")
 
else:
    raise ValueError(f"Unsupported file type: {file_ext}")
 
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rename columns to remove invalid characters for Delta Lake
def sanitize_column_names(df):
    for col_name in df.columns:
        sanitized_name = col_name.replace(" ", "_")  # Replace spaces with underscores
        df = df.withColumnRenamed(col_name, sanitized_name)
    return df

# Sanitize column names
df = sanitize_column_names(df)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Enrich with metadata
df = df.withColumn("source", lit(os.path.basename(source_path))) \
                .withColumn("IsActive", function.when(function.col("rw") == 1, function.lit(1)).otherwise(function.lit(0)))

# get only columns that need to be moved to Silver
df = df.select(
    function.col("LicenseNumber"),
    function.col("Name"),
    function.col("Type"),
    function.col("ExpirationDate"),
    function.col("LastDateUpdated"),
    function.col("LastTimeUpdated"),
    function.col("source"),
    function.col("rw"),
    function.col("IsActive"),
)

df = df.withColumn("silverHash", sha2(concat_ws("||", df.LicenseNumber, df.rw), 256)) \
                .withColumn("loadtime", current_timestamp()) 




display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check if row_hash has generated more than 1 unique id
dupes = (
    df.groupBy("silverHash")
      .count()
      .filter(col("count") > 1)
)

if dupes.count() > 0:
    print("fix dupes")
else:
    print("carry on")

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
