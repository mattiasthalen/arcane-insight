import dlt
import os

import polars as pl
import polars.selectors as cs

import dlt_utilities.cdc_strategy as cdc

from dlt.common.pipeline import current_pipeline, resource_state
from dlt.sources.helpers.transform import add_row_hash_to_table
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
            "primary_key": "id",
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
                    "path": "hearthstone/metadata/gameModes",
                    "paginator": "single_page",
                },
            },
            {
                "name": "raw__hearthstone__bg_game_modes",
                "endpoint": {
                    "path": "hearthstone/metadata/bgGameModes",
                    "paginator": "single_page",
                },
            },
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
    load_dotenv()
    
    pipeline = dlt.pipeline(
        pipeline_name="battle_net",
        destination=dlt.destinations.duckdb("./data/bronze.duckdb"),
        dataset_name="raw",
        #dev_mode=True
    )
    
    primary_key = "id"
    source = battle_net__source()
    resources = source.resources
    
    for resource in resources.items():
        resource_name, resource_object = resource
        print(f"\nProcessing resource: {resource_name}")
        
        # Try and load the destination data
        try:
            destination_df = pl.DataFrame(pipeline.dataset()[resource_name].df())
            destination_exists = True
        
        except dlt.destinations.exceptions.DatabaseUndefinedRelation:
            destination_df = pl.DataFrame()
            destination_exists = False
        
        if pipeline.first_run or not destination_exists:
            print("Mode: Full load")
            source_df = pl.DataFrame(resource_object)
            load_df = cdc.add_hash_to_rows(source_df, source_df.columns)
            
        else:
            print("Mode: CDC detection")

            source_df = pl.DataFrame(resource_object)
            source_df.columns = destination_df.columns
            
            load_df = cdc.extract_cdc_data(              
                source_df=source_df,
                target_df=destination_df,
                key_columns=[primary_key],
                order_by="_dlt_load_id",
                detect_by=source_df.columns,
                descending=True,
                cdc_action_label="_dlt_cdc_action",
                cdc_hash_label="_dlt_cdc_hash"
            )

        load_info = pipeline.run(load_df.to_arrow(), table_name=resource_name)
        print(f"\n{load_info}")
    
if __name__ == "__main__":
    battle_net__load()