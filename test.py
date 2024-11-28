import json
import os
from databricks.sdk.core import Config
from databricks.connect.session import DatabricksSession as SparkSession
import requests
import streamlit as st
import mlflow.deployments
client = mlflow.deployments.get_deploy_client("databricks")


spark = SparkSession.builder.getOrCreate()

spark.sql("SELECT SUM(MilkYieldKg) FROM main.aidev.milk WHERE AnimalIdentifier = 117 AND Date BETWEEN '2020-01-01' AND '2020-03-31'").show()
# for detail in table_details.json():
#     print(detail)
# def sql_template(table_meta_text, column_meta_text):
#     SQL_TEMPLATE = f"""This is the relevant table and column information for building SQL code.

#     CONTEXT TO USE TO CONSTRUCT SQL QUERY:
#     TABLE INFORMATION:
#     {table_meta_text}

#     COLUMN INFORMATION:
#     {column_meta_text}
#     """

#     EXAMPLE_RESPONSE = f""" You are a SQL assistant. Your only job is to write proper SQL code that can be ran in Databricks notebooks. YOU DO NOT DO ANYTHING OTHER THAN WRITE SQL CODE.. Here is an example of how you would respond:

#     <USER'S QUERY>:
#     How many privileges have been granted in total?

#     <YOUR RESPONSE>:
#     SELECT COUNT(*) AS total_privileges
#     FROM information_schema.schema_privileges

#     <USER'S QUERY>:
#     List all distinct privilege types in the catalog.

#     <YOUR RESPONSE>:
#     SELECT DISTINCT privilege_type
#     FROM information_schema.schema_privileges

#     <USER'S QUERY>:
#     How many privileges are granted by each grantor?

#     <YOUR RESPONSE>:
#     SELECT grantor, COUNT(*) AS privileges_granted
#     FROM table_name
#     GROUP BY grantor;

#     """
#     return SQL_TEMPLATE, EXAMPLE_RESPONSE

# def return_sql_response(df_to_list, user_question):

#     RESPONSE_TEMPLATE = f"""
#         You are a nice assistant that organizes information into a summary.

#         The user asked <USER'S QUERY> and the response is given by <DICTIONARY>. You should be conversational and speak as a human. For example, don't mention the dictionary.

#         <DICTIONARY>:
#         {df_to_list}

#         <USER'S QUERY>:
#         {user_question}
#         """
#     return RESPONSE_TEMPLATE

# def get_metadata(catalog, table_name):
#     table_meta = spark.sql(
#         f"select * from {catalog}.information_schema.tables where table_name = '{table_name}'")
#     table_meta_text = table_meta.select('comment').collect()[0]['comment']

#     column_meta = spark.sql(
#         f"select * from {catalog}.information_schema.columns where table_name = '{table_name}'")
#     column_meta_text_preprocess = column_meta.select(
#         'column_name', 'comment', 'data_type').collect()[:]

#     # Initialize a list to store dictionaries for each row
#     data_list = []

#     # Iterate over the sample data and populate the list with dictionaries
#     for row in column_meta_text_preprocess:
#         data_list.append({
#             'column_name': row.column_name,
#             'comment': row.comment,
#             'data_type': row.data_type
#         })
#     # Convert the list of dictionaries to JSON format
#     column_meta_text = json.dumps(data_list, indent=4)
#     return table_meta_text, column_meta_text

# catalog = 'main'  # sample catalog
# table_name1 = 'schema_privileges'  # sample table1

# table_meta_text1, column_meta_text1 = get_metadata(catalog, table_name1)

# table_meta_text = table_meta_text1
# column_meta_text = column_meta_text1

# spark.sql("use catalog main;")
# spark.sql("use database aidev;")

# SQL_TEMPLATE, EXAMPLE_RESPONSE = sql_template(
#     table_meta_text, column_meta_text)

# messages = [
#     {
#         "role": "system",
#         "content": f"({EXAMPLE_RESPONSE})\n({SQL_TEMPLATE})"
#     }

# ]

# messages2 = []
# with open("mydata2.json", "w") as x:
#     json.dump(messages, x, indent=4)

# def main():

#     st.title("Simple Databricks App")

#     query = st.text_input("Ask your question here:")
#     if query:
#         try:
#             messages.append({"role": "user", "content": query})

#             completions_response = client.predict(
#                 endpoint="/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
#                 inputs={
#                     "messages": messages,
#                     "temperature": 0.1,
#                     "max_tokens": 50,
#                     "n": 1
#                 }
#             )
#             sql_result = completions_response.choices[0]['message']['content']

#             filtered_df = spark.sql()
#             df = spark.sql(sql_result)
#             df_to_list = [row.asDict() for row in df.collect()]

#             print(sql_result)

#             RESPONSE_TEMPLATE = return_sql_response(df_to_list, query)

#             messages2.append({"role": "system", "content": RESPONSE_TEMPLATE})
#             messages2.append(
#                 {"role": "user", "content": "Interpret the result please."})

#             completions_final_response = client.predict(
#                 endpoint="/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
#                 inputs={
#                     "messages": messages2,
#                     "temperature": 0.5,
#                     "max_tokens": 500,
#                     "n": 1
#                 }
#             )

#             final_result = completions_final_response.choices[0]['message']['content']

#             st.write(final_result)
#             print(final_result)
#         except Exception as e:
#             st.error(f"An error occurred: {e}")

#             st.write("Feel free to ask another question.")

# if __name__ == "__main__":
#     main()
