from datetime import datetime
import logging
import mlflow.deployments
from fastapi import FastAPI, HTTPException, status
from dotenv import load_dotenv
import requests
import os
from copy import deepcopy

from ..core.utils import generate_system_message_with_metadata
from ..core.spark_session import get_spark_session
from ..core.delta_table_handler import Delta_Table_Handler
from ..models.schemas import InputModel, UserHerdAccessResponse, CustomFormatter

load_dotenv()

app = FastAPI()


handler = logging.StreamHandler()
formatter = CustomFormatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)


logging.basicConfig(level=logging.DEBUG, handlers=[handler])


SYSTEM_MESSAGE = generate_system_message_with_metadata()
spark = get_spark_session()


@app.get("/herd-access")
async def get_user_herd_access():

    print(f"Start with getting all herds {datetime.now()}")

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


@app.post("/ask_the_bot", status_code=status.HTTP_200_OK)
def ask_the_bot_handler(input: InputModel):
    logging.info(f"Received question at {datetime.now()}: {input.question}")

    # Deep copy of the system message for this request
    message = deepcopy(SYSTEM_MESSAGE)
    message.append({"role": "user", "content": input.question})

    try:
        logging.info("Initializing Delta_Table_Handler instance.")
        ai_in_delta_tables = Delta_Table_Handler(
            catalog_name="main",
            client=mlflow.deployments.get_deploy_client("databricks"),
            spark=spark,
            system_messages=message,
            herd_ids=input.herds
        )

        logging.info(f"Query execution started at {datetime.now()}.")
        response = ai_in_delta_tables.execute_query_with_response(
            input.question)
        logging.info(f"Query execution completed at {datetime.now()}.")

        return {"response": response}

    except Exception as e:
        logging.error(f"Error while processing request: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {str(e)}"
        )
