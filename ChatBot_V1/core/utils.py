import json
import os

import requests


def sql_template(table_meta_text, column_meta_text):
    SQL_TEMPLATE = f"""This is the relevant table and column information for building SQL code.

    CONTEXT TO USE TO CONSTRUCT SQL QUERY:
    TABLE INFORMATION:
    {table_meta_text}

    COLUMN INFORMATION:
    {column_meta_text}
    """

    EXAMPLE_RESPONSE = f""" You are a SQL assistant. Your only job is to write proper SQL code that can be ran in Databricks notebooks. YOU DO NOT DO ANYTHING OTHER THAN WRITE SQL CODE.. Here is an example of how you would respond:


    """
    return SQL_TEMPLATE, EXAMPLE_RESPONSE


def return_sql_response(df_to_list, user_question):

    RESPONSE_TEMPLATE = f"""
        You are a nice assistant that organizes information into a summary.

        The user asked <USER'S QUERY> and the response is given by <DICTIONARY>. You should be conversational and speak as a human. For example, don't mention the dictionary.

        <DICTIONARY>:
        {df_to_list}

        <USER'S QUERY>:
        {user_question}
        """
    return RESPONSE_TEMPLATE


def get_metadata(table_name):
    endpoint = f"https://{os.getenv('HOST1')}/api/2.1/unity-catalog/tables/{table_name}"

    headers = {
        "Authorization": f"Bearer {os.getenv('API_TOKEN_DATABRICKS1')}"
    }

    table_meta_text = []

    table_details = requests.get(
        endpoint, headers=headers)

    table_meta_text.append(table_details.json()["comment"])

    data_list = []

    for detail in table_details.json()["columns"]:
        data_list.append({
            'column_name': detail['name'],
            'comment': detail['comment'],
            'data_type': detail['type_text']
        })

    column_meta_text = json.dumps(data_list, indent=4)
    return str(table_meta_text), column_meta_text


def get_system_message():

    endpoint = f"https://{os.getenv('HOST1')}/api/2.1/unity-catalog/tables"

    headers = {
        "Authorization": f"Bearer {os.getenv('API_TOKEN_DATABRICKS1')}"
    }

    params = {
        "catalog_name": "main",
        "schema_name": "aidev"
    }

    list_of_table_names = []

    table_details = requests.get(
        endpoint, headers=headers, params=params)

    for detail in table_details.json()["tables"]:
        list_of_table_names.append(f"main.aidev.{detail['name']}")

    table_meta_text = ""
    column_meta_text = ""

    for table_name in list_of_table_names:
        meta_text_for_each_table, column_meta_text_for_each_table = get_metadata(
            table_name)

        if meta_text_for_each_table != None and column_meta_text_for_each_table != None:

            table_meta_text = table_meta_text + meta_text_for_each_table
            column_meta_text = column_meta_text + column_meta_text_for_each_table

    SQL_TEMPLATE, EXAMPLE_RESPONSE = sql_template(
        table_meta_text, column_meta_text)

    system_message = [
        {
            "role": "system",
            "content": f"({EXAMPLE_RESPONSE})\n({SQL_TEMPLATE})"
        }

    ]

    return system_message
