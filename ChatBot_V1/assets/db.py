from langchain_community.utilities.sql_database import SQLDatabase

import os


def get_db():
    return {
    "milkproduction": "milkproduction",
    "streamerherdoverview": "streamerherdoverview",
}