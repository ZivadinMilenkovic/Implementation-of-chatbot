import logging
from typing import List
from pydantic import BaseModel


class DataModel(BaseModel):
    input: dict[str, List[dict[str, str]]]
    config: dict[str, dict[str, str]]

    @classmethod
    def create_with_content(cls, content: str, session_id: str) -> "DataModel":
        return cls(
            input={"input": [{"content": content, "type": "human"}]},
            config={"configurable": {"session_id": session_id}},
        )


class InputModel(BaseModel):
    question: str
    herds: List[int]


class HerdAccess(BaseModel):
    HerdId: int


class UserHerdAccessResponse(BaseModel):
    HerdIds: List[int]


class CustomFormatter(logging.Formatter):
    # Define color codes
    RESET = "\033[0m"
    COLORS = {
        "DEBUG": "\033[94m",    # Blue
        "INFO": "\033[92m",     # Green
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",    # Red
        "CRITICAL": "\033[95m",  # Magenta
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{log_color}{message}{self.RESET}"
