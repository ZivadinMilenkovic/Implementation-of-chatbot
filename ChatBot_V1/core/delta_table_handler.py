import logging
from .utils import generate_sql_response_template
from .client_setup import get_the_client
import re


class Delta_Table_Handler:
    def __init__(self, catalog_name, spark, client, system_messages, herd_ids):
        self.catalog_name = catalog_name
        self.spark = spark
        self.client = client
        self.system_messages = system_messages
        self.herd_ids = herd_ids

    def execute_query_with_response(self, question: str) -> str:
        """
        Processes a user question to generate a SQL query, execute it, and return the interpreted results.

        This function performs the following steps:
        1. Generates a SQL query based on the user's question using a language model.
        2. Executes the generated SQL query using Spark to retrieve results from the catalog.
        3. Formats the results into a response template.
        4. Requests a final interpretation of the results from a language model.

        Args:
            question (str): The user's question or request that needs to be answered with SQL.

        Returns:
            str: The final interpreted result as a string.
                If an error occurs at any step, a descriptive error message is returned.

        """

        logging.info(
            f"Processing question: '{question}' for catalog: {self.catalog_name}")

        try:
            logging.info("Generating SQL query from system messages.")
            initial_response = get_the_client(
                self.client, self.system_messages, max_tokens=500, temperature=0.1
            )
            generated_sql = initial_response.choices[0]['message']['content']
            logging.info(f"Generated SQL: {generated_sql}")
        except Exception as e:
            logging.error(f"Failed to generate SQL query: {e}")
            raise Exception(f"Error in generating SQL query: {e}")

        try:
            logging.info("Executing SQL query using Spark.")

            herd_filter = f"HerdIdentifier IN ({','.join(map(str, self.herd_ids))})"

            if "WHERE" in generated_sql:
                combined_sql = f"{generated_sql.strip(';')} AND {herd_filter}"
            else:
                combined_sql = f"{generated_sql.strip()} WHERE {herd_filter}"
            print(combined_sql)
            df = self.spark.sql(generated_sql)
            df_to_list = [row.asDict() for row in df.collect()]
            logging.info(
                "SQL query executed successfully and results collected.")
        except Exception as e:
            logging.error(f"Failed to execute SQL query with Spark: {e}")
            raise Exception(f"Error in executing SQL query: {e}")

        try:
            logging.info("Generating response template for SQL results.")
            response_template = generate_sql_response_template(
                df_to_list, question)
        except Exception as e:
            logging.error(f"Failed to generate response template: {e}")
            raise Exception(f"Error in executing SQL query: {e}")

        follow_up_messages = [
            {"role": "system", "content": response_template},
            {"role": "user", "content": "Interpret the result please."},
        ]

        try:
            logging.info("Getting final interpretation of the SQL results.")
            final_response = get_the_client(
                self.client, follow_up_messages, max_tokens=500, temperature=0.5
            )
            final_result = final_response.choices[0]['message']['content']
            logging.info("Final interpretation received successfully.")
        except Exception as e:
            logging.error(f"Failed to get the final interpretation: {e}")
            raise Exception(f"Error in executing SQL query: {e}")

        return final_result
