from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_databricks import ChatDatabricks


def setup_the_llm():

    llm = ChatDatabricks(
        target_uri="databricks",
        endpoint="/serving-endpoints/databricks-meta-llama-3-1-70b-instruct",
        temperature=0,
        max_retries=1,
    )
    return llm


def setup_the_agent(llm, db):
    sql_agent = create_sql_agent(
        llm=llm,
        verbose=True,
        db=db,
        handle_parsing_errors=True,
    )
    return sql_agent