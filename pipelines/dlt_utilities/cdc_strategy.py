import polars as pl
import typing as t

CDC_ACTION_LABEL = "_cdc_action"
CDC_HASH_LABEL = "_cdc_hash"

def cast_lists_to_string(df: pl.DataFrame) -> pl.DataFrame:
    casted_df = df.with_columns(
        [
            pl.format("[{}]", pl.col(col).cast(pl.List(pl.String)).list.join(",")).alias(col)
            if df.schema[col] == pl.List
            else pl.col(col)
            for col in df.columns
        ]
    )
    
    return casted_df
    
def add_hash_to_rows(
    df: pl.DataFrame,
    hash_columns: t.List[str],
    hash_label: str = CDC_HASH_LABEL
) -> pl.DataFrame:
    
    # TODO: move concatenation and hash to seperate functions
    hashed_df = df.with_columns(
        pl.concat_str(
            pl.col(hash_columns),
            separator="|",
            ignore_nulls=True
        )
        .hash()
        .cast(pl.String)
        .str.slice(0, 16)
        .cast(pl.Int64)
        .alias(hash_label)
    )
    
    return hashed_df

def extract_active_records(
    df: pl.DataFrame,
    partition_by: t.List[str],
    order_by: str,
    descending: bool = True,
    cdc_action_label: str = CDC_ACTION_LABEL
) -> pl.DataFrame:
    
    sorted_df = df.sort(by=order_by, descending=descending)
    latest_df = sorted_df.group_by(partition_by).agg(pl.col("*").first())
    active_df = latest_df.filter(pl.col(cdc_action_label) != "DELETE")
    
    return active_df

def detect_action(
    df: pl.DataFrame,
    detect_by: str,
    cdc_action_label: str = CDC_ACTION_LABEL
) -> pl.DataFrame:
    
    left_label = detect_by
    right_label = f"{detect_by}_right"
    
    result_df = df.with_columns(
        pl.when(pl.col(left_label).is_null() & pl.col(right_label).is_null())
        .then(pl.lit("NO_CHANGE"))
        .when(pl.col(right_label).is_null())
        .then(pl.lit("INSERT"))
        .when(pl.col(left_label).is_null())
        .then(pl.lit("DELETE"))
        .when(pl.col(left_label) != pl.col(right_label))
        .then(pl.lit("UPDATE"))
        .otherwise(pl.lit("NO_CHANGE"))
        .alias(cdc_action_label)
    )
    
    return result_df

def coalesce_rows(
    df: pl.DataFrame,
    ignore_columns: list
) -> pl.DataFrame:
    
    coalesce_df = df.select(
        [pl.coalesce(pl.col(col), pl.col(f"{col}_right")) if col not in ignore_columns else pl.col(col) for col in df.columns if '_right' not in col]
    )
    
    return coalesce_df

def extract_cdc_data(
    source_df: pl.DataFrame,
    target_df: pl.DataFrame,
    key_columns: t.List[str],
    detect_by: t.List[str],
    order_by: str,
    descending: bool = True,
    cdc_action_label: str = CDC_ACTION_LABEL,
    cdc_hash_label: str = CDC_HASH_LABEL
) -> pl.DataFrame:
    
    # Add a hash column to the source
    filtered_detect_by = [col for col in detect_by if col not in [order_by, cdc_action_label, cdc_hash_label]]
    hashed_df = add_hash_to_rows(source_df, filtered_detect_by, cdc_hash_label)
    
    # Extract the latest version of records from the target
    active_df = extract_active_records(df=target_df,
        partition_by=key_columns,
        order_by=order_by,
        descending=True
    )
    
    # Combine the source and latest data for comparison
    joined_df = hashed_df.join(
        active_df,
        on=key_columns,
        how="full"
    )
    
    # Detect the changes between the source and target
    action_df = detect_action(joined_df, cdc_hash_label, cdc_action_label)
    
    # Coalesce the source and target data
    coalesce_df = coalesce_rows(
        df=action_df,
        ignore_columns=[cdc_action_label]
    )

    # Remove records that are the same in the source and target
    delta_df = coalesce_df.filter(pl.col(cdc_action_label) != "NO_CHANGE")
    
    return delta_df