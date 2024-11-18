from fastapi import FastAPI, HTTPException, Header, status, Response

import requests

from pydantic import BaseModel

from sqlalchemy.engine import create_engine

from ..assets.db import get_db
from dotenv import load_dotenv
from ChatBot_V1.assets.utils import MultiDataFrameAgentLLM
from ..model import InputModel, HerdAccess, UserHerdAccessResponse

import os


load_dotenv()

app = FastAPI()


engine = create_engine(
    f"databricks+connector://token:{os.getenv('API_TOKEN_DATABRICKS')}@{os.getenv('HOST')}:443/default",
    connect_args={
        "http_path": f"/sql/1.0/warehouses/{os.getenv('WORKHOUSE')}",
    }
)


@app.get("/herd-access", response_model=UserHerdAccessResponse)
def get_user_herd_access(authorization: str = Header(...)):

    token = authorization.split(" ")[1]

    if not token:
        raise HTTPException(status_code=401, detail="Invalid token.")

    url = "https://backoffice.mmmooogle.com/api/v1/userteamherdaccess/me"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code, detail="Error fetching herd access data."
        )

    herd_data = response.json()

    herd_ids = set()

    if isinstance(herd_data, list):
        herd_ids.update(
            herd.get("HerdId") for herd in herd_data if herd.get("HerdId") is not None
        )
    else:
        for herd in herd_data.get("AccessControls", []):
            herd_id = herd.get("HerdId")
            if herd_id is not None:
                herd_ids.add(herd_id)

    return UserHerdAccessResponse(HerdIds=list(herd_ids))


@app.post("/testtest", status_code=status.HTTP_200_OK)
def test(input: InputModel):
    multi_df_agent_llm = MultiDataFrameAgentLLM(get_db(), engine, input.herds)

    response = multi_df_agent_llm.run(input)

    if response.status_code == 422:
        return response

    if response.status_code == 500:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content="User location is not supported for the API use.",
        )

    return response
