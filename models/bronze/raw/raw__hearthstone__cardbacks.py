import os
import pandas as pd
import requests
import time
import typing as t

from datetime import datetime
from dotenv import load_dotenv
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

columns={
    "id": "text",
    "cardbackCategory": "text",
    "text": "text",
    "name": "text",
    "sortCategory": "text",
    "slug": "text",
    "image": "text",
    
    "_sqlmesh__extracted_at": "datetime"
}

@model(
    name='bronze.raw.raw__hearthstone__cardbacks',
    description='Extract & load model for the cardbacks endpoint from the Hearthstone API.',
    cron="*/5 * * * *",
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
    load_dotenv()
    
    token_url = "https://oauth.battle.net/token"
    auth_dict = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("BATTLE_NET__CLIENT_ID"),
        "client_secret": os.getenv("BATTLE_NET__CLIENT_SECRET"),
    }
    
    token_response = requests.post(token_url, data=auth_dict)
    token_response.raise_for_status()
    access_token = token_response.json().get("access_token")
    
    # Fetch paginated data
    base_url = "https://eu.api.blizzard.com/hearthstone/cardbacks"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"locale": "en_US", "pageSize": 500, "page": 1}

    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    page_count = data.get("pageCount", 1)
    
    all_keys = set()

    for page_num in range(1, page_count + 1):
        print(f"Fetching page {page_num}/{page_count}...")
        params["page"] = page_num
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("cardBacks", [])
        
        if results:
            df = pd.DataFrame(results)
            
            # Add missing columns using pd.NA
            missing_columns = set(columns.keys()) - set(df.columns)
            
            for column in missing_columns:
                print(f"Adding missing column: {column}")
                df[column] = pd.NA
            
            df["_sqlmesh__extracted_at"] = execution_time
            
            yield df
            
            all_keys.update(df.columns)
    
    # print(all_keys)