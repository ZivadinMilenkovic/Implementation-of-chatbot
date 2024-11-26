import os
from langchain_community.utilities import SQLDatabase


def get_dataframes():
    return {
        "milk": "milk",
        "animal_info": "animal_info",
    }
