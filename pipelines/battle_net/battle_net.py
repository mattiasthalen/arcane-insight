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
                    "params": {
                        "pageSize": 500,
                    }
                },
            },
            {
                "name": "raw__hearthstone__metadata",
                "endpoint": {
                    "path": "hearthstone/metadata",
                    "paginator": "single_page",
                },
            },
        ]
    }

    yield from rest_api_resources(config)

def load_battle_net() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="battle_net",
        destination=dlt.destinations.duckdb("./data/bronze/battle_net.duckdb"),
        dataset_name="api",
    )

    load_info = pipeline.run(battle_net__source())
    print(load_info)

if __name__ == "__main__":
    load_battle_net()
