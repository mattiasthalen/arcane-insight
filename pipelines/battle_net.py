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
    
    load_info = pipeline.run(battle_net__source())
    print(load_info)

if __name__ == "__main__":
    load_dotenv()
    battle_net__load()