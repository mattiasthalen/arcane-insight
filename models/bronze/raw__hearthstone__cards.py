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

columns={
    "armor": "text",
    "artistName": "text",
    "attack": "text",
    "bannedFromSideboard": "text",
    "cardSetId": "text",
    "cardTypeId": "text",
    "childIds": "text",
    "classId": "text",
    "collectible": "text",
    "copyOfCardId": "text",
    "cropImage": "text",
    "durability": "text",
    "flavorText": "text",
    "health": "text",
    "id": "text",
    "image": "text",
    "imageGold": "text",
    "isZilliaxCosmeticModule": "text",
    "isZilliaxFunctionalModule": "text",
    "keywordIds": "text",
    "manaCost": "text",
    "maxSideboardCards": "text",
    "minionTypeId": "text",
    "multiClassIds": "text",
    "multiTypeIds": "text",
    "name": "text",
    "parentId": "text",
    "rarityId": "text",
    "runeCost": "text",
    "slug": "text",
    "spellSchoolId": "text",
    "text": "text",
    "touristClassId": "text",
    
    "_sqlmesh__extracted_at": "datetime"
}

@model(
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
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("BATTLE_NET__CLIENT_ID"),
        "client_secret": os.getenv("BATTLE_NET__CLIENT_SECRET"),
    }
    
    token_response = requests.post(token_url, data=data)
    token_response.raise_for_status()
    access_token = token_response.json().get("access_token")
    
    # Fetch paginated data
    base_url = "https://eu.api.blizzard.com/hearthstone/cards"
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
        results = data.get("cards", [])
        
        if results:
            df = pd.DataFrame(results)
            all_keys.update(df.columns)
            
            # Add missing columns using pd.NA
            missing_columns = set(columns.keys()) - set(df.columns)
            
            for column in missing_columns:
                print(f"Adding missing column: {column}")
                df[column] = pd.NA
            
            df["_sqlmesh__extracted_at"] = execution_time
            
            yield df
            
    print(f"Discovered keys: {all_keys}")