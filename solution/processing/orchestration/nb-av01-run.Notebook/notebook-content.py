# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-run 
# **Purpose:** orchestration of all notebooks in ELTL pipeline. Notebook used for orchestration to make it a lot simpler to handle: 
# - Environment configuration between different notebooks. This is the only notebook in which the Environment is configured, and all other notebooks use the same session (and therefore environment).  
# - No need to hard-code Notebook IDs into a Variable Library. 
# 
# **Note:** not metadata-driven at this stage to keep control - but could easily be folded into the metadata framework - if that's your preference. 

# CELL ********************

notebookutils.notebook.run("nb-av01-0-ingest-api", 300)

notebookutils.notebook.run("nb-av01-1-load", 300)

notebookutils.notebook.run("nb-av01-2-clean", 300)

notebookutils.notebook.run("nb-av01-3-model", 300)

notebookutils.notebook.run("nb-av01-4-validate", 300)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
