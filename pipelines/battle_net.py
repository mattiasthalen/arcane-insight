import dlt
import os

from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from typing import Any, Optional
from dotenv import load_dotenv

def print_script_name() -> None:
    print(f"Running script: {os.path.basename(__file__)}")

@dlt.source(name="battle_net")
def battle_net__source(credentials = dlt.secrets.value) -> Any:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://eu.api.blizzard.com/",
            "auth": {
                "type": "oauth2_client_credentials",
                "access_token_url": "https://oauth.battle.net/token",
                "client_id": credentials["client_id"],
                "client_secret": credentials["client_secret"],
            },

        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 0,
            "endpoint": {
                "params": {
                    "locale": "en_US",
                },
            },
        },
        "resources": [
            {
                "name": "raw__hearthstone__cards",
                "primary_key": "id",
                "endpoint": {
                    "path": "hearthstone/cards",
                    "params": {
                        "pageSize": 500,
                    },
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "total_path": "pageCount",
                    },
                },
            },
            {
                "name": "raw__hearthstone__cardbacks",
                "endpoint": {
                    "path": "hearthstone/cardbacks",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__metadata",
                "endpoint": {
                    "path": "hearthstone/metadata",
                    "paginator": "single_page",
                },
            },            
            {
                "name": "raw__hearthstone__sets",
                "endpoint": {
                    "path": "hearthstone/metadata/sets",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__set_groups",
                "endpoint": {
                    "path": "hearthstone/metadata/setGroups",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__types",
                "endpoint": {
                    "path": "hearthstone/metadata/types",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__rarities",
                "endpoint": {
                    "path": "hearthstone/metadata/rarities",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__classes",
                "endpoint": {
                    "path": "hearthstone/metadata/classes",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__minion_types",
                "endpoint": {
                    "path": "hearthstone/metadata/minionTypes",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__keywords",
                "endpoint": {
                    "path": "hearthstone/metadata/keywords",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__game_modes",
                "endpoint": {
                    "path": "hearthstone/metadata/game_modes",
                    "paginator": "single_page",
                },
            },
            # {
            #     "name": "raw__hearthstone__bg_game_modes",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/bgGameModes",
            #         "paginator": "single_page",
            #     },
            # },
            {
                "name": "raw__hearthstone__cardback_categories",
                "endpoint": {
                    "path": "hearthstone/metadata/cardBackCategories",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__mercenary_factions",
                "endpoint": {
                    "path": "hearthstone/metadata/mercenaryFactions",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__mercenary_roles",
                "endpoint": {
                    "path": "hearthstone/metadata/mercenaryRoles",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__spell_schools",
                "endpoint": {
                    "path": "hearthstone/metadata/spellSchools",
                    "paginator": "single_page",
                },
            },
            
            
        ]
    }

    yield from rest_api_resources(config)

def battle_net__load() -> None:
    print_script_name()
    
    pipeline = dlt.pipeline(
        pipeline_name="battle_net",
        destination=dlt.destinations.duckdb("./data/bronze.duckdb"),
        dataset_name="raw",
        dev_mode=True
    )
    
    source = battle_net__source()
    
    # if pipeline.first_run:
    #     print("First run")
        
    #     load_info = pipeline.run(source)
    #     print(load_info)
    #     return None
    
    resources = source.resources
    
    for resource_name, resource_object in resources.items():
        print(f"Processing resource: {resource_name}")
        
        incoming_data = 
        print(list(resource_object))
        
        resource = pipeline.dataset()[resource_name]
        print(resource)
    
    #

def process_resource_data_with_cdc(data: list, resource_name: str) -> list:
    """
    Process resource data with CDC logic.
    It only appends new, updated, or deleted rows.
    """
    current_pipeline = dlt.current.pipeline()
    transformed_data = []  # List to hold transformed records (insert, update, delete)
    deleted_ids = set()  # To track deleted IDs
    active_ids = set()  # To track active IDs
    existing_ids = set()  # To track existing IDs (all ids from the destination)

    # Check if it's the first run or not
    if not current_pipeline.first_run:
        # Query the destination dataset for the most recent cdc_action for each id
        resource_table = current_pipeline.dataset().get(resource_name)

        if resource_table:  # Only proceed if the table exists in the destination
            # Sort by _dlt_load_id DESC and select the first record for each id
            latest_cdc_df = resource_table.select("id", "_dlt_load_id", "cdc_action") \
                .sort("_dlt_load_id", ascending=False)

            # For each id, get the most recent cdc_action
            latest_cdc_df = latest_cdc_df.groupby("id").agg(dlt.first("cdc_action").alias("latest_action")).df()

            # All IDs present in the destination
            existing_ids = set(latest_cdc_df["id"])

            # Determine active IDs (IDs where latest cdc_action != 'delete')
            active_ids = set(latest_cdc_df[latest_cdc_df["latest_action"] != "delete"]["id"])

            # Calculate deleted_ids as IDs that exist in the destination but are not in the incoming data
            incoming_ids = set(record["id"] for record in data)  # Get active ids from incoming data
            deleted_ids = existing_ids - incoming_ids

            # Handle deletions (IDs that are missing in the new data but exist in the destination)
            # Filter the resource table to get all the records that need to be flagged as deleted
            deleted_records = resource_table.filter(resource_table["id"].isin(deleted_ids)).df()
            deleted_records["cdc_action"] = "delete"

            # Append all deleted records to the transformed data
            transformed_data.extend(deleted_records.to_dict(orient="records"))

    # Process the incoming data (new or updated records)
    for record in data:
        pk = record["id"]

        if pk in deleted_ids:
            # It was deleted previously, so treat it as an insert now
            record["cdc_action"] = "insert"
            transformed_data.append(record)
        elif pk in active_ids:
            # Existing active record, flag as an update
            record["cdc_action"] = "update"
            transformed_data.append(record)
        else:
            # New record, append to the list
            record["cdc_action"] = "insert"
            transformed_data.append(record)

    return transformed_data

if __name__ == "__main__":
    load_dotenv()
    battle_net__load()