import json
import logging
import os

import requests


def create_sql_template(table_metadata, column_metadata):
    """
    Creates a SQL template and an example response for the assistant.

    Args:
        table_metadata (str): Metadata of the tables as a formatted string.
        column_metadata (str): Metadata of the columns as a formatted string.

    Returns:
        tuple: A SQL_TEMPLATE string containing the structure for generating SQL queries
               and an EXAMPLE_RESPONSE string illustrating an example assistant response.
    """

    logging.info(
        "Creating SQL template using provided table and column metadata.")
    SQL_TEMPLATE = f"""This is the relevant table and column information for building SQL code.

    CONTEXT TO USE TO CONSTRUCT SQL QUERY:
    TABLE INFORMATION:
    {table_metadata}

    COLUMN INFORMATION:
    {column_metadata}
    """

    EXAMPLE_RESPONSE = f""" You are a SQL assistant. Your only job is to write proper SQL code that can be ran in Databricks notebooks. In this format where we get only query. YOU DO NOT DO ANYTHING OTHER THAN WRITE SQL CODE.. Here is an example of how you would respond:
    <USER'S QUERY>:
    How much herd do we have

    <YOUR RESPONSE>:
    SELECT COUNT(DISTINCT HerdIdentifier) AS Number_of_Herds
    FROM main.aidev.animal_info;
    
    <USER'S QUERY>:
    How much milk does cow 117 give us from Januar 2020 to March 2020
    
    <YOUR RESPONSE>:
    SELECT SUM(MilkYieldKg) AS TotalMilkYield
    FROM main.aidev.milkings
    WHERE AnimalIdentifier = 117
    AND Date >= '2020-01-01'
    AND Date <= '2020-03-31'
    
    <USER'S QUERY>:
    What is the average milking duration
    
    <YOUR RESPONSE>:
    SELECT AVG(TotalTimeMilking) AS AverageMilkingDuration
    FROM main.aidev.milkings;

    <USER'S QUERY>:
    Which animal did give us most milk in January 2020
    
    <YOUR RESPONSE>:
    SELECT AnimalIdentifier, SUM(MilkYieldKg) AS TotalMilkYield
    FROM main.aidev.milkings
    WHERE Date >= '2020-01-01'
    AND Date <= '2020-01-31'
    GROUP BY AnimalIdentifier
    ORDER BY TotalMilkYield DESC
    LIMIT 1

    
    
    """
    logging.info("SQL template and example response created successfully.")
    return SQL_TEMPLATE, EXAMPLE_RESPONSE


def generate_sql_response_template(df_to_list, user_question):
    """
    Generates a response template for the assistant based on a user's question and a data summary.

    Args:
        df_to_list (list): A list of dictionaries representing the query result.
        user_question (str): The question or request made by the user.

    Returns:
        str: A response template that integrates the user's query and the formatted query result.
    """

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
    """
    Fetches metadata for a specific table from the Databricks Unity Catalog API.

    Args:
        table_name (str): The fully qualified name of the table (e.g., catalog.schema.table).

    Returns:
        tuple: 
            - A string containing the table comment (or empty if not available).
            - A JSON-formatted string containing the metadata of the table's columns.
    """
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
    """
    Generates a system message by aggregating metadata from all tables in the specified schema.

    Retrieves table and column metadata from the Databricks Unity Catalog API, 
    creates SQL templates using the metadata, and compiles it into a system message.

    Returns:
        list: A list of dictionaries representing the system message, including:
              - An example assistant response.
              - A SQL template for generating SQL queries.
    """

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
