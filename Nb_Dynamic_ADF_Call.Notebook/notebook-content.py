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

# PARAMETERS CELL ********************

# Lets just setup a simple if block to spit out a value based on what the ADF pipeline wants
external_parameter = 1 # Set to default, ADF will pass in the value for us. 1 will be the default
value_to_return = "" # Set to blank, depending on what is passed in will determine what is returned

def return_value(external_parameter):
    if external_parameter == 1:
        value_to_return = "One"
    elif external_parameter == 2:
        value_to_return = "Two"
    elif external_parameter == 3:
        value_to_return = "Three"
    else:
        value_to_return = "Unknown"
    # Return to our ADF?
    return value_to_return


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
