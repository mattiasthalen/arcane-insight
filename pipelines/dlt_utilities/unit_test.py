import polars as pl
import cdc_strategy as cdc
import numpy as np

def test_add_hash_to_rows():
    hash_label = "hash"
    df = pl.DataFrame([{"a": 1, "b": [1, 2, 3], "c": {"x": 1, "y": 2}}])
    
    hashed_df = cdc.add_hash_to_rows(df, df.columns, hash_label)
    
    assert hash_label in hashed_df.columns
    assert hashed_df[hash_label].dtype == pl.Int64
    assert np.max(hashed_df[hash_label].to_list()) < np.int64(10e15)

def test_extract_cdc_data():
    
    cdc_action_label = "_cdc_action"
    cdc_hash_label = "_cdc_hash"
    
    source_df = pl.DataFrame(
        [
            {"id": 0, "value": "A", "_dlt_load_id": 3},
            {"id": 1, "value": "A", "_dlt_load_id": 3},
            {"id": 2, "value": "B", "_dlt_load_id": 3},
            {"id": 4, "value": "A", "_dlt_load_id": 3},
        ]
    )
    
    target_df = pl.DataFrame(
        [
            {"id": 0, "value": "A", "_dlt_load_id": 1, cdc_action_label: "INSERT"},
            {"id": 2, "value": "A", "_dlt_load_id": 1, cdc_action_label: "INSERT"},
            {"id": 3, "value": "A", "_dlt_load_id": 1, cdc_action_label: "INSERT"},
            {"id": 4, "value": "A", "_dlt_load_id": 1, cdc_action_label: "INSERT"},
            {"id": 4, "value": "A", "_dlt_load_id": 2, cdc_action_label: "DELETE"},
        ]
    )

    hash_columns = ["id", "value"]
    hashed_target_df = cdc.add_hash_to_rows(
        df=target_df,
        hash_columns=hash_columns,
        hash_label=cdc_hash_label
    )
    
    # Run the CDC detection
    result_df = cdc.extract_cdc_data(
        source_df=source_df,
        target_df=hashed_target_df,
        key_columns=["id"],
        detect_by=source_df.columns,
        order_by="_dlt_load_id",
        descending=True,
        cdc_action_label=cdc_action_label,
        cdc_hash_label=cdc_hash_label
    ) 
    
    # Assert the expected CDC actions
    actual_values = result_df.select(cdc_action_label).to_series().to_list()   
    assert actual_values == ["INSERT", "UPDATE", "INSERT", "DELETE"]
    
    assert cdc_action_label in result_df.columns
    assert cdc_hash_label in result_df.columns