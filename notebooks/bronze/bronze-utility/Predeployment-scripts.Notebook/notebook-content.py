# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Repeatable commands

# CELL ********************

# Check if the schema 'bronze' exists, and create it if not
if not spark.catalog.databaseExists("bronze"):
    spark.sql("CREATE SCHEMA bronze")
    print("Schema 'bronze' created.")
else:
    print("Schema 'bronze' already exists.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
