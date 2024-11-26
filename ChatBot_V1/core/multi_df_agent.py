from typing import Dict
from .setup_agent import setup_the_llm, setup_the_agent


class MultiDataFrameAgentLLM:
    def __init__(self, dataframes: Dict[str, "DataFrame"], herd_ids, spark):

        self.llm = setup_the_llm()
        self.dataframes = dataframes
        self.herd_ids = herd_ids
        self.spark = spark

    def run(self, query: str) -> str:

        selected_df_key = self.select_dataframe_using_llm(query).strip()
        selected_df = self.dataframes.get(selected_df_key)

        df = self.spark.sql(
            f"""SELECT * FROM {selected_df} WHERE HerdIdentifier IN ({','.join(map(str, self.herd_ids))})"""
        )

        if selected_df_key:
            agent = setup_the_agent(self.llm, df)

            result = agent.invoke(query)

            summary_prompt = f"""
            The user asked: '{query}'.
            The agent's answer was: '{result}'.
            Summarize this answer in a contextual style that provides information related to the herd and animals, 
            so the user gets a complete and understandable response.
            """
            summary_response = self.llm.invoke(summary_prompt)

            return summary_response.content if summary_response else result
        else:
            return f"No matching DataFrame found for the query: {query}"

    def select_dataframe_using_llm(self, query: str) -> str:

        prompt = f"""
        I have the following DataFrames available: {list(self.dataframes.keys())}.
        The user asked: '{query}'.
        Which DataFrame should be used for this query? Respond with only the name of the DataFrame.
        """
        response = self.llm.invoke(prompt)

        df_key = response.content

        if df_key:
            return df_key
        else:
            return None
