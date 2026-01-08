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

import os

# Establish some file parameters
srcPath = "/lakehouse/bronze/Tables/drivers"
dstPath = "lakehouse/silver/drivers"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create temp views for merge
spark.read.format("delta").load(srcPath).createOrReplaceTempView("src_drivers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *

# Create schema
schema = StructType([
    StructField("LicenseNumber", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("ExpirationDate", TimestampType(), True),
    StructField("LastDateUpdated", TimestampType(), True),
    StructField("LastTimeUpdated", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read file data into bronze table
df = spark.read.format("delta") \
    .option("header", "true") \
    .schema(schema) \
    .load(path)

# Write to bronze
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.drivers")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
