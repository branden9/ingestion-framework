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

# ## 1. Call the UDF to return a specified variable

# CELL ********************

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
retrieve = path_function.get_variable(
    variableName="sensitiveValue" #get variableName from the var-library
)

print(retrieve)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. We can retrieve variables directly from the library as well if desired

# CELL ********************

variable_library = notebookutils.variableLibrary.getLibrary("var-library")
variable = variable_library.getVariable("sensitiveValue")
display(variable)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Retrieve a keyvault secret, this will maintain a full layer of secrecy, likely better than anything we could use a var library for

# CELL ********************

# Specify the resource we are calling
path_function = notebookutils.udf.getFunctions("UDF_POC")

# Pass in param values for the function to use
kv_url = path_function.get_keyvault_url(keyVaultUrl="KEY_VAULT_URL")

# Store URL in var library, pass in here
secret_Value = mssparkutils.credentials.getSecret(kv_url, 'test-secret')

# Print to show the [REDACTED] value
print(secret_Value)
display(secret_Value)

# Interesting test case...
if secret_Value == "S3crets!":
    print("secret found")
else:
    print("secret kept secret")


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
