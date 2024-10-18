
from fastapi import FastAPI,status,Response
import requests

from ..llm import setup_the_llm
from ..model import DataModel,InputModel
from langserve import add_routes
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

add_routes(
    app,
    setup_the_llm(),
)

@app.post("/testtest",status_code=status.HTTP_200_OK)
def test(input:InputModel):
    formated_input = DataModel.create_with_content(str(input.input),str(input.session_id)).model_dump()
    
    response=requests.post("http://0.0.0.0:8000/invoke/",json=formated_input)

    if(response.status_code == 422):
        return response
    
    if(response.status_code == 500):    
        return Response(status_code=status.HTTP_400_BAD_REQUEST,content="User location is not supported for the API use.")
     
    return response['output']