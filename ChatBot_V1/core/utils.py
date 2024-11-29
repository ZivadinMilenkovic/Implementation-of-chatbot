import json
import logging
import os

import requests


def create_sql_template(table_metadata, column_metadata):
    logging.info(
        "Creating SQL template using provided table and column metadata.")
    SQL_TEMPLATE = f"""This is the relevant table and column information for building SQL code.

    CONTEXT TO USE TO CONSTRUCT SQL QUERY:
    TABLE INFORMATION:
    {table_metadata}

    COLUMN INFORMATION:
    {column_metadata}
    """

    EXAMPLE_RESPONSE = f""" You are a SQL assistant. Your only job is to write proper SQL code that can be ran in Databricks notebooks. YOU DO NOT DO ANYTHING OTHER THAN WRITE SQL CODE.. Here is an example of how you would respond:


    """
    logging.info("SQL template and example response created successfully.")
    return SQL_TEMPLATE, EXAMPLE_RESPONSE


def generate_sql_response_template(df_to_list, user_question):
    logging.info("Generating response template for the user's SQL question.")

    RESPONSE_TEMPLATE = f"""
        You are a nice assistant that organizes information into a summary.

        The user asked <USER'S QUERY> and the response is given by <DICTIONARY>. You should be conversational and speak as a human. For example, don't mention the dictionary.

        <DICTIONARY>:
        {df_to_list}

        <USER'S QUERY>:
        {user_question}
        """

    logging.info("Response template generated successfully.")
    return RESPONSE_TEMPLATE


def fetch_table_metadata(table_name):
    endpoint = f"https://{os.getenv('HOST1')}/api/2.1/unity-catalog/tables/{table_name}"

    headers = {
        "Authorization": f"Bearer {os.getenv('API_TOKEN_DATABRICKS1')}"
    }

    logging.info(
        f"Fetching metadata for table: {table_name} from Databricks Unity Catalog API.")

    try:
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch metadata for table {table_name}: {e}")
        return None, None

    table_details = response.json()
    table_meta_text = table_details.get("comment", "")
    logging.info(f"Fetched table comment: {table_meta_text}")

    column_meta_list = [
        {
            'column_name': column['name'],
            'comment': column.get('comment', 'No comment provided'),
            'data_type': column.get('type_text', 'Unknown type')
        }
        for column in table_details.get("columns", [])
    ]

    column_meta_text = json.dumps(column_meta_list, indent=4)
    logging.info(f"Extracted column metadata for table {table_name}.")

    return str(table_meta_text), column_meta_text


def generate_system_message_with_metadata():

    endpoint = f"https://{os.getenv('HOST1')}/api/2.1/unity-catalog/tables"

    headers = {
        "Authorization": f"Bearer {os.getenv('API_TOKEN_DATABRICKS1')}"
    }

    params = {
        "catalog_name": "main",
        "schema_name": "aidev"
    }

    logging.info("Fetching table details from Databricks Unity Catalog API")

    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch table details: {e}")
        return []

    table_names = [
        f"main.aidev.{detail['name']}" for detail in response.json().get("tables", [])]

    logging.info(f"Retrieved table list from schema 'aidev': {table_names}")

    tables_metadata = ""
    column_metadata = ""

    for table_name in table_names:
        table_meta, column_meta = fetch_table_metadata(
            table_name)

        if table_meta != None and column_meta != None:

            tables_metadata = tables_metadata + table_meta
            column_metadata = column_metadata + column_meta

    logging.info(f"Metadata fetched for tables\n{table_names}")

    SQL_TEMPLATE, EXAMPLE_RESPONSE = create_sql_template(
        tables_metadata, column_metadata)

    logging.info(
        "System message created successfully")

    system_message = [
        {
            "role": "system",
            "content": f"({EXAMPLE_RESPONSE})\n({SQL_TEMPLATE})"
        }

    ]

    return system_message
