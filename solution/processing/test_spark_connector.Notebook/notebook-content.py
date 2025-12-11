# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

url = (
    f"jdbc:sqlserver://{variables.METADATA_SERVER}:1433;"
    f"database={variables.METADATA_DB};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30;"
    f"authentication=ActiveDirectoryMSI"
)

df = spark.read.format("jdbc").option("url", url).option("dbtable", "metadata.source_store").load()
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
