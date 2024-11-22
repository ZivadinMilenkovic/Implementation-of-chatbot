from ..llm_agent import setup_the_llm, setup_the_agent
# from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit

class MultiDataFrameAgentLLM:
    def __init__(self, db):

        self.llm = setup_the_llm()
        self.db = db


    def run(self, query: str):
        
        # toolkit = SQLDatabaseToolkit(db =  llm = self.llm)
       
        agent = setup_the_agent(self.llm, self.db)

        result = agent.invoke(query)
        print(result)

        return result["output"]

