import dlt

from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from typing import Any, Optional


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
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "total_path": None,
                    },
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
        ]
    }

    yield from rest_api_resources(config)

def load_battle_net() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="battle_net",
        destination=dlt.destinations.duckdb("data/arcane_insight.duckdb"),
        # destination=dlt.destinations.filesystem(
        #     bucket_url="data/bronze/",
        # ),
        dataset_name="bronze",
    )

    load_info = pipeline.run(battle_net__source(), loader_file_format="parquet")
    print(load_info)

if __name__ == "__main__":
    load_battle_net()
