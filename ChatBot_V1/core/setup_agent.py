from langchain_experimental.agents.agent_toolkits import create_spark_dataframe_agent
from langchain_databricks import ChatDatabricks
from langchain.agents import AgentType


def setup_the_llm():
    return ChatDatabricks(
        target_uri="databricks",
        endpoint="/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
        temperature=0,
        max_retries=1,
    )


def setup_the_agent(llm, spark_df):
    agent = create_spark_dataframe_agent(
        llm=llm,
        df=spark_df,
        verbose=True,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        allow_dangerous_code=True,
    )

    return agent
