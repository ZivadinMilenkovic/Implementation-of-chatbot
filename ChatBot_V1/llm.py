import os

from langchain.agents import AgentType
from langchain_google_genai import ChatGoogleGenerativeAI

from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_databricks import ChatDatabricks
from langchain.agents import AgentExecutor
def setup_the_llm():

    llm = ChatDatabricks(
        target_uri="databricks",
        endpoint="/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
        temperature=0,
        max_retries=2
    )
    return llm


def setup_the_agent(llm, selected_df_pd):
    agent = create_pandas_dataframe_agent(
        llm=llm,
        verbose=True,
        df=selected_df_pd,
        allow_dangerous_code=True,
        handle_parsing_errors=True,
        
    )
    return agent
