from datetime import datetime
from fastapi import FastAPI, HTTPException, status

import requests

from ..assets.db import get_databricks_hive_metastore
from ChatBot_V1.assets.multi_data_frame_agent_llm import MultiDataFrameAgentLLM
from dotenv import load_dotenv
from ..model import InputModel, UserHerdAccessResponse
import os


load_dotenv()

app = FastAPI()

multi_df_agent_llm = MultiDataFrameAgentLLM(get_databricks_hive_metastore())

@app.get("/herd-access")
async def get_user_herd_access():
    print(f"Start with getting all herds{datetime.now()}")
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

    try:
        auth_response = requests.post(auth_url, json=auth_payload)
        
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
                    
        print(f"Finish with getting all herds{datetime.now()}")

        return UserHerdAccessResponse(HerdIds=list(herd_ids))

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=getattr(e.response, "status_code", 500), detail=str(e)
        )


@app.post("/testtest", status_code=status.HTTP_200_OK)
def test(input: InputModel):

    print(f"Start with getting answer from llm{datetime.now()}")

    response = multi_df_agent_llm.run(input.question)

    return response