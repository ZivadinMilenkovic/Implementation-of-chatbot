import os

from langchain.agents import AgentType
from langchain_google_genai import ChatGoogleGenerativeAI

from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent


def setup_the_llm():

    llm = ChatGoogleGenerativeAI(
        model="gemini-1.5-flash-8b",
        api_key=os.getenv("API_TOKEN_GEMINI"),
        temperature=0,
        max_retries=2,
        handle_parsing_errors=True,
    )
    return llm


def setup_the_agent(llm, selected_df_pd):
    agent = create_pandas_dataframe_agent(
        llm=llm,
        df=selected_df_pd,
        verbose=True,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        allow_dangerous_code=True,
    )

    return agent
