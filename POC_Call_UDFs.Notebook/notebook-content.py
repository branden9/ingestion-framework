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
from pyspark.sql.functions import substring

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
    sourceSchema="bronze", #used if building more dynamic path
    dstLakehouse="silver_lkh_id", #variable name from library, will retrieve the stored value
    targetType="lakehouse", #can be lakehouse, warehouse, files
    targetStorageType="Tables", #can be files, Tables
    targetSchema="bronze", #used if building more dynamic path
    targetName="mytable", #File or table name
    schemaParse=False #Default to false
)

# Create a dataframe off of the values returned from the func
df = spark.createDataFrame([retrieve], schema=["srcpath", "dstpath", "deltapath", "sourceStorageType"])

# Now set params for re-useability
source_path = df.select("srcpath").first()[0] #grab first to be safe
destination_path = df.select("dstpath").first()[0] #grab first to be safe
deltapath = df.select("deltapath").first()[0] #grab first to be safe
storageType = df.select("sourceStorageType").first()[0] #grab first to be safe

# Display as parameter values in our notebook
print(source_path)
print(destination_path)
print(deltapath)
print(storageType)

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

# The problem here is, if we mask, the actual string is masked, which makes this rather worthless
if retrieve == "masked-fabric":
    print("secret found")
else:
    print("secret hidden")

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

# MARKDOWN ********************

# ## 4. We can retrieve variables directly from the library as well if desired

# CELL ********************

variable_library = notebookutils.variableLibrary.getLibrary("var-library")
variable = variable_library.getVariable("maskedValue")
display(variable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Retrieve a keyvault secret, this will maintain a full layer of secrecy, likely better than anything we could use a var library for

# CELL ********************

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
kv_url = path_function.get_keyvault_url(keyVaultUrl="KEY_VAULT_URL")

# Store URL in var library, pass in here
secret_Value = mssparkutils.credentials.getSecret(kv_url, 'test-secret')

# Print to show the [REDACTED] value
print(secret_Value)


# Interesting test case...
if secret_Value == "S3crets!":
    print("secret found")
else:
    print("secret kept secret")

# Once done, erase
#del secret_Value ##physical removal



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a DataFrame with the parameter
df = spark.createDataFrame([(secret_Value,)], ["param_exposed"])

# Show param in df
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
