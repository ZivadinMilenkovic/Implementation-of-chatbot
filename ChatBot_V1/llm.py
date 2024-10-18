import re
from pathlib import Path
from typing import Callable, Union
import os

from fastapi import HTTPException

from langchain.agents import AgentType
from langchain_community.chat_message_histories import FileChatMessageHistory
from langchain_core.chat_history import BaseChatMessageHistory
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits import create_sql_agent
from langchain_core.runnables.history import RunnableWithMessageHistory

from ChatBot_V1.db.db import get_db


def _is_valid_identifier(value: str) -> bool:
    valid_characters = re.compile(r"^[a-zA-Z0-9-_]+$")
    return bool(valid_characters.match(value))


def create_session_factory(
    base_dir: Union[str, Path],
) -> Callable[[str], BaseChatMessageHistory]:
    base_dir_ = Path(base_dir) if isinstance(base_dir, str) else base_dir
    if not base_dir_.exists():
        base_dir_.mkdir(parents=True)

    def get_chat_history(session_id: str) -> FileChatMessageHistory:
        if not _is_valid_identifier(session_id):
            raise HTTPException(
                status_code=400,
                detail=f"Session ID `{session_id}` is not in a valid format. "
                "Session ID must only contain alphanumeric characters, "
                "hyphens, and underscores.",
            )
        file_path = base_dir_ / f"{session_id}.json"
        return FileChatMessageHistory(str(file_path))

    return get_chat_history

def setup_the_llm():

    llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash-8b",
    api_key=os.getenv("API_TOKEN_GEMINI"),
    temperature=0,
    max_retries=2,
    handle_parsing_errors=True
)

    agent = create_sql_agent(llm=llm,db=get_db(),agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION)

    agent_with_chat_history = RunnableWithMessageHistory(
    agent,
    create_session_factory("chat_histories"),
    input_messages_key="input",
    history_messages_key="chat_history",
    )
    
    return agent_with_chat_history