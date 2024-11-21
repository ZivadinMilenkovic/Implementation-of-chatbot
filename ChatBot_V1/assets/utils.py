from typing import Dict
import pandas as pd
from ..llm import setup_the_llm, setup_the_agent
from langchain.schema import OutputParserException
class MultiDataFrameAgentLLM:
    def __init__(self, dataframes: Dict[str, "DataFrame"], engine, herd_ids):

        self.llm = setup_the_llm()
        self.dataframes = dataframes
        self.engine = engine
        self.herd_ids = herd_ids

    def run(self, query: str) -> str:

        selected_df_key = self.select_dataframe_using_llm(query).strip()
        selected_df = self.dataframes.get(selected_df_key)

        connection = self.engine.raw_connection()

        selected_df_pd = pd.read_sql_query(
            f"SELECT * FROM {selected_df} WHERE HerdIdentifier IN ({','.join(map(str, self.herd_ids))})",
            con=connection,
        )

        if selected_df_key:
            agent = setup_the_agent(self.llm, selected_df_pd)

            result = agent.run(query)

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
