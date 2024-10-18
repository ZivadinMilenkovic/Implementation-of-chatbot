from langchain_community.utilities.sql_database import SQLDatabase

import os


def get_db():
    return SQLDatabase.from_databricks(
        catalog = "hive_metastore", 
        schema = "default",
        host = os.getenv("HOST"),
        api_token = os.getenv("API_TOKEN_DATABRICKS"),
        cluster_id = os.getenv("CLUSTER_ID")
    )