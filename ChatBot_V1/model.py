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
    input: str


class HerdAccess(BaseModel):
    HerdId: int


class UserHerdAccessResponse(BaseModel):
    HerdIds: List[int]
