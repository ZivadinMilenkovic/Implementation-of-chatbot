
from fastapi import FastAPI,status,Response
from sqlalchemy.engine import create_engine

from ..assets.db import get_db
from dotenv import load_dotenv
from ChatBot_V1.assets.utils import MultiDataFrameAgentLLM
from ..model import InputModel
import os

load_dotenv()

app = FastAPI()

engine = create_engine(
    f"databricks+connector://token:{os.getenv("API_TOKEN_DATABRICKS")}@{os.getenv("HOST")}:443/default",
    connect_args={
        "http_path": f"/sql/1.0/warehouses/{os.getenv('WORKHOUSE')}",
    }
)

@app.post("/testtest",status_code=status.HTTP_200_OK)
def test(input:InputModel):
    multi_df_agent_llm = MultiDataFrameAgentLLM(get_db(),engine,733)
    
    response = multi_df_agent_llm.run(input)

    if(response.status_code == 422):
        return response
    
    if(response.status_code == 500):    
        return Response(status_code=status.HTTP_400_BAD_REQUEST,content="User location is not supported for the API use.")
     
    return response