import polars as pl

def add_hash_to_rows(df: pl.DataFrame, label: str = "hash") -> pl.DataFrame:
    return df.with_columns(pl.struct(pl.all()).hash().alias(label))

def collect_set(df: pl.DataFrame, column: str) -> set:
    return set(df.select(column).to_series().to_list())

def set_cdc_action(df: pl.DataFrame, action: str) -> pl.DataFrame:
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
    key_name: str
) -> pl.DataFrame:
    
    keys = collect_set(destination_df.filter(pl.col("_cdc_action") == "delete"), key_name)
    filter = pl.col(key_name).is_in(keys)
    filtered_df = source_df.filter(filter)
    df = set_cdc_action(filtered_df, "reinsert")
    
    return df

def collect_rows_to_update(
    source_df: pl.DataFrame,
    key_name: str,
    source_keys: set,
    destination_keys: set,
    source_hashes: set,
    destination_hashes: set,
    reinsert_hashes: set
) -> pl.DataFrame:
    
    updated_pks = source_keys.intersection(destination_keys)
    updated_hashes = source_hashes - destination_hashes - reinsert_hashes
    filter = pl.col(key_name).is_in(updated_pks) & pl.col("_cdc_hash").is_in(updated_hashes)
    filtered_df = source_df.filter(filter)
    df = set_cdc_action(filtered_df, "update")
    
    return df

def collect_rows_to_delete(
    destination_df: pl.DataFrame,
    key_name: str,
    source_keys: set,
    destination_keys,
    cdc_action_name: str = "_cdc_action"
) -> pl.DataFrame:
    
    deleted_pks = destination_keys-source_keys
    filter = pl.col(key_name).is_in(deleted_pks) & ~(pl.col(cdc_action_name) == "delete")
    filtered_df = destination_df.filter(filter)
    df = set_cdc_action(filtered_df, "delete")
    
    return df
    
def generate_cdc_delta(
    source_df: pl.DataFrame,
    destination_df: pl.DataFrame,
    primary_key: str = "id"
) -> pl.DataFrame:
    
    # Add hash to source rows
    hashed_source_df = add_hash_to_rows(source_df, "_cdc_hash")
    
    # Collect primary keys & hashes
    source_pks = collect_set(hashed_source_df, primary_key)
    destination_pks = collect_set(destination_df, primary_key)
    source_hashes = collect_set(hashed_source_df, "_cdc_hash")
    destination_hashes = collect_set(destination_df, "_cdc_hash")
    
    # Create delta dataframe
    insert_df = collect_rows_to_insert(hashed_source_df, primary_key, source_pks, destination_pks)
    reinsert_df = collect_rows_to_reinsert(hashed_source_df, destination_df, primary_key)
    delete_df = collect_rows_to_delete(destination_df, primary_key, source_pks, destination_pks, "_cdc_action")
    
    update_df = collect_rows_to_update(
        hashed_source_df,
        primary_key,
        source_pks,
        destination_pks,
        source_hashes,
        destination_hashes,
        collect_set(reinsert_df, "_cdc_hash")
    )
    
    delta_df = pl.concat([insert_df, reinsert_df, update_df, delete_df])
    
    return delta_df

destination_df = pl.DataFrame(
    [
        {"id": 1, "name": "Alice", "value": 100, "_cdc_hash": None, "_cdc_action": "insert"},
        {"id": 2, "name": "Bob", "value": 200, "_cdc_hash": None, "_cdc_action": "insert"},
        {"id": 3, "name": "Charlie", "value": 300, "_cdc_hash": None, "_cdc_action": "insert"},
        {"id": 3, "name": "Charlie", "value": 300, "_cdc_hash": None, "_cdc_action": "delete"},
        {"id": 4, "name": "David", "value": 400, "_cdc_hash": None, "_cdc_action": "insert"},
        {"id": 5, "name": "Eve", "value": 500, "_cdc_hash": None, "_cdc_action": "insert"},
        {"id": 5, "name": "Eve", "value": 500, "_cdc_hash": None, "_cdc_action": "delete"},
    ]
)

destination_df = add_hash_to_rows(destination_df, "_cdc_hash")

source_df = pl.DataFrame(
    [
        {"id": 1, "name": "Alice", "value": 120},
        {"id": 2, "name": "Bob", "value": 300},
        {"id": 5, "name": "Eva", "value": 500},
        {"id": 6, "name": "Kurt", "value": 500},
        {"id": 7, "name": "Bella", "value": 500}
    ]
)

delta_df = generate_cdc_delta(source_df, destination_df)

print(delta_df)