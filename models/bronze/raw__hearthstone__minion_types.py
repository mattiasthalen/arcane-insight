import os
import pandas as pd
import requests
import time
import typing as t

from datetime import datetime
from dotenv import load_dotenv
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

load_dotenv()

# Get this script name as the model name
current_file_path = os.path.relpath(__file__, os.getcwd())
model_name = '.'.join(os.path.splitext(current_file_path)[0].split(os.sep)[1:])

columns={
    "id": "text",
    "slug": "text",
    "name": "text",
    "gameModes": "text",
    
    "_sqlmesh__extracted_at": "datetime"
}

@model(
    name=model_name,
    description='Extract & load model for the minion endpoint from the Hearthstone API.',
    kind=dict(
        name=ModelKindName.FULL,
    ),
    columns=columns
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:
    
    # Authorization
    token_url = "https://oauth.battle.net/token"
    auth_dict = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("BATTLE_NET__CLIENT_ID"),
        "client_secret": os.getenv("BATTLE_NET__CLIENT_SECRET"),
    }
    
    token_response = requests.post(token_url, data=auth_dict)
    token_response.raise_for_status()
    access_token = token_response.json().get("access_token")
    
    # Fetch data
    base_url = "https://eu.api.blizzard.com/hearthstone/metadata/minionTypes"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"locale": "en_US"}

    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    
    df = pd.DataFrame(data)
    df["_sqlmesh__extracted_at"] = execution_time
    
    yield df