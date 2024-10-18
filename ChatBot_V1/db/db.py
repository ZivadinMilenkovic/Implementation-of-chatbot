from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.utilities.spark_sql import SparkSQL

import os


def get_db():
    return SparkSQL(schema="default")
        # catalog = "hive_metastore", 
        # schema = "default",
        # host = os.getenv("HOST"),
        # api_token = os.getenv("API_TOKEN_DATABRICKS"),
        # cluster_id = os.getenv("CLUSTER_ID")
    