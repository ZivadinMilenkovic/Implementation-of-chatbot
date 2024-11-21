from ..llm import setup_the_llm, setup_the_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

class MultiDataFrameAgentLLM:
    def __init__(self, db):

        self.llm = setup_the_llm()
        self.db = db


    def run(self, query: str):
        
        toolkit = SQLDatabaseToolkit(db = self.db, llm = self.llm)
       
        agent = setup_the_agent(self.llm, toolkit)

        result = agent.run(query)
        print(result)

        return result

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
