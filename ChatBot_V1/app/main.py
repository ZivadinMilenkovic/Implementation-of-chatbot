from fastapi import FastAPI, HTTPException
import requests
from pydantic import BaseModel
import os

app = FastAPI()

# Model za prikazivanje liste krda
class HerdAccess(BaseModel):
    HerdId: int
    HerdName: str

class UserHerdAccessResponse(BaseModel):
    herds: list[HerdAccess]

# Funkcija za dobijanje Auth0 tokena
def get_auth_token():
    url = "https://bovinet.auth0.com/oauth/ro"
    payload = {
        "client_id": "jJDqhts1jJ0q9rtxFNcNPFXBqXReHyva",
        "username": "bigdata@mmmooogle.com",  # Tajna
        "password": "PSmYbrdv5jSWBPG5wrb",  # Tajna
        "id_token": "",
        "connection": "Username-Password-Authentication",
        "grant_type": "password",
        "scope": "openid app_metadata profile perms",
        "device": ""
    }
    
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Error obtaining auth token.")
    
    return response.json().get("id_token")

# Endpoint za povlačenje krda sa kojima korisnik ima pristup
@app.get("/herd-access", response_model=UserHerdAccessResponse)
def get_user_herd_access():
    token = get_auth_token()

    # Proveri da li je token važeći
    if not token:
        raise HTTPException(status_code=401, detail="Invalid token.")

    # GET zahtev ka backoffice API-u koristeći dobijeni id_token
    url = "https://backoffice.mmmooogle.com/api/v1/userteamherdaccess/me"
    headers = {"Authorization": f"Bearer {token}"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Error fetching herd access data.")
    
    # Preuzimanje i formatiranje odgovora
    herd_data = response.json()

    # Ispis podataka o krdu
    print("Herd Data:", herd_data)  # Dodaj ovo za debagovanje
    
    unique_herds = {}
    
    # Proveri da li je herd_data lista
    if isinstance(herd_data, list):
        for herd in herd_data:
            herd_id = herd.get("HerdId")
            herd_name = herd.get("HerdName", "Nepoznat")  # Postavi podrazumevanu vrednost
            if herd_id is not None:  # Uveri se da HerdId postoji
                unique_herds[herd_id] = HerdAccess(HerdId=herd_id, HerdName=herd_name)
    else:
        for herd in herd_data.get("AccessControls", []):
            herd_id = herd.get("HerdId")
            herd_name = herd.get("HerdName", "Nepoznat")  # Postavi podrazumevanu vrednost
            if herd_id is not None:  # Uveri se da HerdId postoji
                unique_herds[herd_id] = HerdAccess(HerdId=herd_id, HerdName=herd_name)

    # Pretvori rečnik u listu
    herds = list(unique_herds.values())

    return UserHerdAccessResponse(herds=herds)


# Ostale rute i logika vezana za chatbota su komentarisane
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
