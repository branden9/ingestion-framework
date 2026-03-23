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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from notebookutils import mssparkutils, notebook
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, lit, col, count
from pyspark.sql import functions 
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_path = "abfss://26f84b3b-c936-4482-b883-db691ee83597@onelake.dfs.fabric.microsoft.com/307568c6-a5f2-4bd1-9b58-34f495d97fe8/Tables/dbo/bikeLanding"
deltapath = "Tables/bikeLanding"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").load(source_path)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the primary keys to use, if any
source_keys = "dateHash"


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
elif sourceStorageType == "Tables":
    df = spark.read.format("delta").load(deltapath)
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
        sanitized_name = col_name.replace(" ", "")  # Replace spaces with underscores
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

keys = [key.strip() for key in source_keys.split(",")]
merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in keys])

display(merge_condition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create hash for silver layer
df = df.withColumn("dateHash", sha2(concat_ws("||", df.Date), 256)) 


# Now get our distinct columns we want to load to this table
distinct_columns = [
    "Date"
    , "dateHash"
    ]


# Update df with just the columns from above list, get distinct
df = df.select(distinct_columns).distinct()



display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Correct SQL syntax for aliasing columns in Spark SQL uses 'AS', not '='
# Also, remove square brackets from column names!

# Create the temp view if not already created (uncomment if needed):
df.createOrReplaceTempView("source_view")

df = spark.sql("""
    SELECT 
        Date
        ,YEAR(Date) AS Year
        ,MONTH(Date) AS Month
        ,WEEKOFYEAR(Date) AS Week
        ,DAYOFWEEK(Date) AS DayOfWeek
        ,DAY(Date) AS Day
        ,QUARTER(Date) AS Quarter
        ,DAYOFMONTH(Date) AS DayOfMonth
        ,DAYOFYEAR(Date) AS DayOfYear
        ,CASE WHEN DayOfWeek IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS DayType
        ,CASE WHEN DayOfWeek(Date) = 1 THEN 'Sunday'
              WHEN DayOfWeek(Date) = 2 THEN 'Monday'
              WHEN DayOfWeek(Date) = 3 THEN 'Tuesday'
              WHEN DayOfWeek(Date) = 4 THEN 'Wednesday'
              WHEN DayOfWeek(Date) = 5 THEN 'Thursday'
              WHEN DayOfWeek(Date) = 6 THEN 'Friday'
              WHEN DayOfWeek(Date) = 7 THEN 'Saturday'
         END AS DayName
         ,dateHash
    FROM source_view
                """)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check if row_hash has generated more than 1 unique id
dupes = (
    df.groupBy("dateHash")
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

    # for bronze load, we want to be insert only
    delta_table.alias("target").merge(
        df.alias("source"),
        merge_condition
    ).withSchemaEvolution() \
     .whenNotMatchedInsertAll() \
     .whenMatchedUpdateAll() \
     .execute()

    print("Data merged into existing table.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
