# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from notebookutils import mssparkutils, notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Call the first UDF, this will generate paths and return them to us to use as a parameter in the notebook

# CELL ********************

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
retrieve = path_function.get_variable_name(
    workspaceid="workspace_id", #variable name from library, will retrieve the stored value
    srcLakehouse="bronze_lkh_id", #variable name from library, will retrieve the stored value
    sourceType="lakehouse", #can be lakehouse, warehouse, files
    sourceStorageType="Files", #can be files, Tables
    sourceSubfolder="taxi-raw", #subfolder if from files
    sourceName="medallion-drivers-active.csv", #File or table name
    dstLakehouse="silver_lkh_id", #variable name from library, will retrieve the stored value
    targetType="lakehouse", #can be lakehouse, warehouse, files
    targetStorageType="Tables", #can be files, Tables
    targetSchema="bronze", #used if building more dynamic path
    targetName="mytable", #File or table name
    schemaParse=False #Default to false
)

# Create a dataframe off of the values returned from the func
df = spark.createDataFrame([retrieve], schema=["srcpath", "dstpath", "deltapath"])

# Now set params for re-useability
source_path = df.select("srcpath").first()[0] #grab first to be safe
destination_path = df.select("dstpath").first()[0] #grab first to be safe
deltapath = df.select("deltapath").first()[0] #grab first to be safe

# Display as parameter values in our notebook
print(source_path)
print(destination_path)
print(deltapath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Call the second UDF, this will return a variable to us from the Var library but it will be masked.

# CELL ********************

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
retrieve = path_function.get_masked_variable(
    maskedVariable="maskedValue" #variable name from library, will retrieve stored value
)

print(retrieve)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Call the third UDF, this will return a variabl to us from the Var library but it will be masked if the variable is in the masked_Variable list

# CELL ********************

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
retrieve = path_function.dynamic_masked_variable(
    variableName="unmaskedValue" #variable name from library, will retrieve stored value
)

print(retrieve)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
