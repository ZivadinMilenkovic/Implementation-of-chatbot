from .utils import return_sql_response
from .client_setup import get_the_client


class AskTheDeltaTable:
    def __init__(self, catalog_name, spark, client, system_messages):
        self.catalog_name = catalog_name
        self.spark = spark
        self.client = client
        self.system_messages = system_messages

    def run(self, question: str) -> str:

        messages2 = []

        completions_response = get_the_client(
            self.client, self.system_messages, 200, 0.1)

        sql_result = completions_response.choices[0]['message']['content']

        print(sql_result)

        df = self.spark.sql(sql_result)
        df_to_list = [row.asDict() for row in df.collect()]

        RESPONSE_TEMPLATE = return_sql_response(
            df_to_list, question)

        messages2.append({"role": "system", "content": RESPONSE_TEMPLATE})
        messages2.append(
            {"role": "user", "content": "Interpret the result please."})

        completions_final_response = get_the_client(
            self.client, messages2, 200, 0.5)

        final_result = completions_final_response.choices[0]['message']['content']

        return final_result
