import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

@dlt.source
def source(credentials=dlt.secrets.value):
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
                    "paginator": "page_number",
                },
                "name": "raw__hearthstone__cardbacks",
                "endpoint": {
                    "path": "hearthstone/cardbacks",
                    "paginator": "page_number",
                },
                "name": "raw__hearthstone__decks",
                "endpoint": {
                    "path": "hearthstone/deck",
                    "paginator": "single_page",
                },
                "name": "raw__hearthstone__metadata",
                "endpoint": {
                    "path": "hearthstone/metadata",
                    "paginator": "single_page",
                },
            },
        ],
    }

    yield from rest_api_resources(config)

def load() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="battle_net",
        destination=dlt.destinations.duckdb("../data/landing_zone.duckdb"),
        dataset_name="battle_net",
    )

    load_info = pipeline.run(source())
    print(load_info)

if __name__ == "__main__":
    load()
