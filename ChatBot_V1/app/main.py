from fastapi import FastAPI, HTTPException, Header
import requests
from pydantic import BaseModel
from ChatBot_V1.model import HerdAccess, UserHerdAccessResponse
app = FastAPI()


@app.get("/herd-access", response_model=UserHerdAccessResponse)
def get_user_herd_access(authorization: str = Header(...)):
    # Token dobijamo iz headera
    token = authorization.split(" ")[1]

    if not token:
        raise HTTPException(status_code=401, detail="Invalid token.")

    url = "https://backoffice.mmmooogle.com/api/v1/userteamherdaccess/me"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Error fetching herd access data.")

    herd_data = response.json()

    # Koristimo set da eliminiramo duplikate
    herd_ids = set()

    if isinstance(herd_data, list):
        herd_ids.update(herd.get("HerdId") for herd in herd_data if herd.get("HerdId") is not None)
    else:
        for herd in herd_data.get("AccessControls", []):
            herd_id = herd.get("HerdId")
            if herd_id is not None:
                herd_ids.add(herd_id)

    return UserHerdAccessResponse(HerdIds=list(herd_ids))

# from ..llm import setup_the_llm
# from ..model import DataModel, InputModel
# from langserve import add_routes
# from dotenv import load_dotenv
#
# load_dotenv()
#
# add_routes(
#     app,
#     setup_the_llm(),
# )
#
# @app.post("/testtest", status_code=status.HTTP_200_OK)
# def test(input: InputModel):
#     formated_input = DataModel.create_with_content(str(input.input), str(input.session_id)).model_dump()
#     
#     response = requests.post("http://0.0.0.0:8000/invoke/", json=formated_input)
#
#     if response.status_code == 422:
#         return response
#     
#     if response.status_code == 500:    
#         return Response(status_code=status.HTTP_400_BAD_REQUEST, content="User location is not supported for the API use.")
#      
#     return response['output']
