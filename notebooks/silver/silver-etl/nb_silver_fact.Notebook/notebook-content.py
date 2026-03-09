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

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
retrieve = path_function.get_variable_name(
    workspaceid="workspace_id", #variable name from library, will retrieve the stored value
    srcLakehouse="bronze_lkh_id", #variable name from library, will retrieve the stored value
    sourceType="lakehouse", #can be lakehouse, warehouse, files
    sourceStorageType="Tables", #can be files, Tables
    sourceSchema="bronze", #used if building more dynamic path
    sourceSubfolder="retail-sales", #subfolder if from files
    sourceName="retailSales", #File or table name
    dstLakehouse="silver_lkh_id", #variable name from library, will retrieve the stored value
    targetType="lakehouse", #can be lakehouse, warehouse, files
    targetStorageType="Tables", #can be files, Tables
    targetSchema="bronze", #used if building more dynamic path
    targetName="product", #File or table name
    schemaParse=False #Default to false
)

# Create a dataframe off of the values returned from the func
df = spark.createDataFrame([retrieve], schema=["srcpath", "dstpath", "deltapath", "soureceStorageType"])

# Now set params for re-useability
source_path = df.select("srcpath").first()[0] #grab first to be safe
target_path = df.select("dstpath").first()[0] #grab first to be safe
deltapath = df.select("deltapath").first()[0] #grab first to be safe
sourceStorageType= df.select("soureceStorageType").first()[0] #grab first to be safe

# Display as parameter values in our notebook
print(source_path)
print(target_path)
print(deltapath)
print(sourceStorageType)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the primary keys to use, if any
source_keys = "productHash"


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
df = df.withColumn("productHash", sha2(concat_ws("||", df.ProductCategory), 256))


# Now get our distinct columns we want to load to this table
distinct_columns = [
    "ProductCategory"
    , "productHash"
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
        ProductCategory 
        ,productHash
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
    df.groupBy("productHash")
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
