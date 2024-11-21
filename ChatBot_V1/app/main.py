from fastapi import FastAPI, HTTPException, Header, status, Response

import requests

from pydantic import BaseModel

from sqlalchemy.engine import create_engine

from ..assets.db import get_db
from ChatBot_V1.assets.utils import MultiDataFrameAgentLLM
from dotenv import load_dotenv
from ..model import InputModel, HerdAccess, UserHerdAccessResponse
import logging
import os

load_dotenv()

app = FastAPI()


engine = create_engine(
    f"databricks+connector://token:{os.getenv('API_TOKEN_DATABRICKS1')}@{os.getenv('HOST1')}:443/default",
    connect_args={
        "http_path": f"/sql/1.0/warehouses/{os.getenv('WORKHOUSE1')}",
    },
)


@app.get("/herd-access")
async def get_user_herd_access():

    auth_url = "https://bovinet.auth0.com/oauth/ro"
    auth_payload = {
        "client_id": os.getenv("CLIENT_ID"),
        "username": os.getenv("USERNAME_OUTH"),
        "password": os.getenv("PASSWORD"),
        "id_token": "",
        "connection": "Username-Password-Authentication",
        "grant_type": "password",
        "scope": "openid app_metadata profile perms",
        "device": "",
    }
    print(auth_payload)
    try:
        auth_response = requests.post(auth_url, json=auth_payload)
        print(auth_response)
        auth_response.raise_for_status()
        auth_data = auth_response.json()
        token = auth_data.get("id_token")

        if not token:
            raise HTTPException(
                status_code=401, detail="Failed to get authentication token"
            )

        herd_url = "https://backoffice.mmmooogle.com/api/v1/userteamherdaccess/me"
        headers = {"Authorization": f"Bearer {token}"}

        herd_response = requests.get(herd_url, headers=headers)
        logging.info(herd_response)
        if herd_response.status_code != 200:
            raise HTTPException(
                status_code=herd_response.status_code,
                detail="Error fetching herd access data.",
            )

        herd_data = herd_response.json()
        herd_ids = set()

        if isinstance(herd_data, list):
            herd_ids.update(
                herd.get("HerdId")
                for herd in herd_data
                if herd.get("HerdId") is not None
            )
        else:
            for herd in herd_data.get("AccessControls", []):
                herd_id = herd.get("HerdId")
                if herd_id is not None:
                    herd_ids.add(herd_id)

        return UserHerdAccessResponse(HerdIds=list(herd_ids))

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=getattr(e.response, "status_code", 500), detail=str(e)
        )


@app.post("/testtest", status_code=status.HTTP_200_OK)
def test(input: InputModel):
    multi_df_agent_llm = MultiDataFrameAgentLLM(get_db(), engine, input.herds)

    response = multi_df_agent_llm.run(input)


    if type(response) != str:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content="Failed to get response from the API.",
        )

    return response
