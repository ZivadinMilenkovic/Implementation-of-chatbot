import os
from langchain_community.utilities import SQLDatabase

def get_databricks_hive_metastore():
    db = SQLDatabase.from_databricks(
        catalog="hive_metastore", 
        schema="default",
        host=os.getenv('HOST1'),
        api_token=os.getenv("API_TOKEN_DATABRICKS1"),
        cluster_id=os.getenv("CLUSTER_ID")
    )
    return db
