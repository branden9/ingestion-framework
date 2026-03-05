import fabric.functions as fn
import pandas as pd 




udf = fn.UserDataFunctions()


# Connect to a variable library and build our dynamic paths needed for ETL between files/tables and/or different lakehouses
@udf.connection(argName="varLib", alias="varlibrary")
@udf.function()
def get_variable_name(
    workspaceid: str,
    srcLakehouse: str, 
    sourceType: str,
    sourceStorageType: str,
    sourceSubfolder: str,
    sourceName: str,
    sourceSchema: str,
    dstLakehouse: str,
    targetType: str,
    targetStorageType: str,
    targetSchema: str,
    targetName: str,
    schemaParse: bool,
    varLib: fn.FabricVariablesClient) -> str:

    # Pull the active value set once (dict[str, str]) and return just the requested key.
    workspace = varLib.getVariables()  # Provided by Fabric's Variable Library binding
    #set the variable
    workspace = workspace[workspaceid]

    src = varLib.getVariables()  # Provided by Fabric's Variable Library binding
    #set the variable
    fromlakehouse = src[srcLakehouse]

    dst = varLib.getVariables()  # Provided by Fabric's Variable Library binding
    #set the variable
    tolakehouse = dst[dstLakehouse]

    # set dynamically
    if sourceStorageType == "Tables":
        if schemaParse is True:
            source_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{fromlakehouse}/{sourceStorageType}/{sourceSchema}_{sourceName}"
        else:
            source_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{fromlakehouse}/{sourceStorageType}/{sourceName}"


    # set dynamically
    if schemaParse is True:
        target_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{tolakehouse}/{targetStorageType}/{targetSchema}_{targetName}"
    else:
        target_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{tolakehouse}/{targetStorageType}/{targetName}"

    # set if source is table, use bellow for df lkp
    if sourceStorageType == "Tables":
        delta_source = f"{sourceStorageType}/{sourceName}"
    else:
        delta_source = "Empty" # overkill?

    return source_path, target_path, delta_source, sourceStorageType


# Connect to a variable library, return a variable but mask the value
@udf.connection(argName="varLib", alias="varlibrary")
@udf.function()
def get_masked_variable(
    maskedVariable: str,
    varLib: fn.FabricVariablesClient) -> str:

    # Pull the active value set once (dict[str, str]) and return just the requested key.
    getVariable = varLib.getVariables()

    #set the variable that we want
    maskedVariable = getVariable[maskedVariable]

    #Mask the variable upon return
    return "*" * len(maskedVariable)


# Connect to a variable library, return a variable and mask the value based on if the variable name is in a list of variables to keep hidden
@udf.connection(argName="varLib", alias="varlibrary")
@udf.function()
def dynamic_masked_variable(
    variableName: str,
    varLib: fn.FabricVariablesClient) -> str:

    # Set list of variables we need to keep hidden from our library
    masked_list = ["maskedValue"]

    # Pull the active value set once (dict[str, str]) and return just the requested key.
    getVariable = varLib.getVariables() 

    # set the variable that we want
    variable = getVariable[variableName]

    # Mask the variable if the variable name is in the list
    if variableName in masked_list:
        myvariable = "*" * len(variable)
    else:
        myvariable = variable 

    # Return the variable, masked or not
    return myvariable



# Connect to a variable library, return a variable and mask the value based on if the variable name is in a list of variables to keep hidden
@udf.connection(argName="varLib", alias="varlibrary")
@udf.function()
def get_keyvault_url(
    varLib: fn.FabricVariablesClient,
    keyVaultUrl: str = "KEY_VAULT_URL") -> str: ##if you are going to pass in default values, the parameter needs to be at the end of the list

    # Retrieve the var library
    getVariable = varLib.getVariables()

    # Select the url we need
    get_kv_url = getVariable[keyVaultUrl]

    # Return the variable, masked or not
    return get_kv_url







