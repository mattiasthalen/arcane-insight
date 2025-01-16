import dlt
import os

from dlt.common.pipeline import current_pipeline, resource_state
import polars as pl

#from dlt.common.schema.schema import is_new_table
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from typing import Any, Optional
from dotenv import load_dotenv

def print_script_name() -> None:
    print(f"Running script: {os.path.basename(__file__)}")
    
def add_hash_to_rows(df: pl.DataFrame, label: str = "_dlt_cdc_hash") -> pl.DataFrame:
    return df.with_columns(pl.struct(pl.all()).hash().alias(label))

def collect_set(df: pl.DataFrame, column: str) -> set:
    return set(df.select(column).to_series().to_list())

def set_cdc_action(df: pl.DataFrame, action: str, label: str = "_dlt_cdc_action") -> pl.DataFrame:
    return df.with_columns(_cdc_action=pl.lit(action))
    
def collect_rows_to_insert(
    source_df: pl.DataFrame,
    key_name: str,
    source_keys: set,
    destination_keys
) -> pl.DataFrame:
    
    keys = source_keys-destination_keys
    filter = pl.col(key_name).is_in(keys)
    filtered_df = source_df.filter(filter)
    df = set_cdc_action(filtered_df, "insert")
    
    return df
    
def collect_rows_to_reinsert(
    source_df: pl.DataFrame,
    destination_df: pl.DataFrame,
    key_name: str,
    cdc_hash_label: str = "_dlt_cdc_hash",
    cdc_action_label: str = "_dlt_cdc_action"
) -> pl.DataFrame:
    
    keys = collect_set(destination_df.filter(pl.col(cdc_action_label) == "delete"), key_name)
    filter = pl.col(key_name).is_in(keys)
    filtered_df = source_df.filter(filter)
    df = set_cdc_action(filtered_df, "reinsert", cdc_action_label)
    
    return df

def collect_rows_to_update(
    source_df: pl.DataFrame,
    key_name: str,
    source_keys: set,
    destination_keys: set,
    source_hashes: set,
    destination_hashes: set,
    reinsert_hashes: set,
    cdc_hash_label: str = "_dlt_cdc_hash",
    cdc_action_label: str = "_dlt_cdc_action"
) -> pl.DataFrame:
    
    updated_pks = source_keys.intersection(destination_keys)
    updated_hashes = source_hashes - destination_hashes - reinsert_hashes
    filter = pl.col(key_name).is_in(updated_pks) & pl.col(cdc_hash_label).is_in(updated_hashes)
    filtered_df = source_df.filter(filter)
    df = set_cdc_action(filtered_df, "update", cdc_action_label)
    
    return df

def collect_rows_to_delete(
    destination_df: pl.DataFrame,
    key_name: str,
    source_keys: set,
    destination_keys,
    cdc_action_label: str = "_dlt_cdc_action"
) -> pl.DataFrame:
    
    deleted_pks = destination_keys-source_keys
    filter = pl.col(key_name).is_in(deleted_pks) & ~(pl.col(cdc_action_label) == "delete")
    filtered_df = destination_df.filter(filter)
    df = set_cdc_action(filtered_df, "delete", cdc_action_label)
    
    return df
    
def generate_cdc_delta(
    source_df: pl.DataFrame,
    destination_df: pl.DataFrame,
    primary_key: str = "id",
    cdc_hash_label: str = "_dlt_cdc_hash",
    cdc_action_label: str = "_dlt_cdc_action"
) -> pl.DataFrame:
    
    # Add hash to source rows
    hashed_source_df = add_hash_to_rows(source_df, cdc_hash_label)
    
    # Collect primary keys & hashes
    source_pks = collect_set(hashed_source_df, primary_key)
    destination_pks = collect_set(destination_df, primary_key)
    source_hashes = collect_set(hashed_source_df, cdc_hash_label)
    destination_hashes = collect_set(destination_df, cdc_hash_label)
    
    # Create delta dataframe
    insert_df = collect_rows_to_insert(hashed_source_df, primary_key, source_pks, destination_pks)
    reinsert_df = collect_rows_to_reinsert(hashed_source_df, destination_df, primary_key)
    delete_df = collect_rows_to_delete(destination_df, primary_key, source_pks, destination_pks, cdc_action_label)
    
    update_df = collect_rows_to_update(
        hashed_source_df,
        primary_key,
        source_pks,
        destination_pks,
        source_hashes,
        destination_hashes,
        collect_set(reinsert_df, cdc_hash_label)
    )
    
    delta_df = pl.concat([insert_df, reinsert_df, update_df, delete_df])
    
    return delta_df

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
            # {
            #     "name": "raw__hearthstone__cardbacks",
            #     "endpoint": {
            #         "path": "hearthstone/cardbacks",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__metadata",
            #     "endpoint": {
            #         "path": "hearthstone/metadata",
            #         "paginator": "single_page",
            #     },
            # },            
            # {
            #     "name": "raw__hearthstone__sets",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/sets",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__set_groups",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/setGroups",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__types",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/types",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__rarities",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/rarities",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__classes",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/classes",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__minion_types",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/minionTypes",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__keywords",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/keywords",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__game_modes",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/game_modes",
            #         "paginator": "single_page",
            #     },
            # },
            # # {
            # #     "name": "raw__hearthstone__bg_game_modes",
            # #     "endpoint": {
            # #         "path": "hearthstone/metadata/bgGameModes",
            # #         "paginator": "single_page",
            # #     },
            # # },
            # {
            #     "name": "raw__hearthstone__cardback_categories",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/cardBackCategories",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__mercenary_factions",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/mercenaryFactions",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__mercenary_roles",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/mercenaryRoles",
            #         "paginator": "single_page",
            #     },
            # },
            # {
            #     "name": "raw__hearthstone__spell_schools",
            #     "endpoint": {
            #         "path": "hearthstone/metadata/spellSchools",
            #         "paginator": "single_page",
            #     },
            # },
            
            
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
    
    current_pipeline = dlt.current.pipeline()
    
    source = battle_net__source()
    resources = source.resources
    
    source_datasets = pipeline.dataset()
    destination_datasets = current_pipeline.dataset()
    
    cdc_hash_label = "_dlt_cdc_hash"
    cdc_action_label = "_dlt_cdc_action"
    
    for resource in resources:
        print(f"Processing resource: {resource}")
        
        source_data = source_datasets[resource]
        destination_data = destination_datasets[resource]
        
        source_df = pl.DataFrame(source_data.df())
        destination_df = pl.DataFrame(destination_data.df())
        
        print("Source data:")
        print(source_df.head())
        
        print("Destination data:")
        print(destination_df.head())
        
        if not pipeline.first_run:
            print("First run")
            hashed_source_df = add_hash_to_rows(source_df)
            action_source_df = set_cdc_action(hashed_source_df, "insert", cdc_action_label)

            print("Action source data:")
            print(action_source_df.head())
            #print(action_source_df.select(["id", cdc_hash_label, cdc_action_label]).head())
        
        return None
        
        # Need flag to see if the resource has been loaded before
        resource_state = dlt.current.resource_state()
        
        print(resource_state)
        #print(destination_df.head())
        
        """
        df = generate_cdc_delta(
            source_df=source_df
            destination_df=destination_df
            primary_key=primary_key
            cdc_hash_label=cdc_hash_label
            cdc_action_label=cdc_action_label
        )
        
        #resource_object = df
        yield df
        """
    
if __name__ == "__main__":
    battle_net__load()