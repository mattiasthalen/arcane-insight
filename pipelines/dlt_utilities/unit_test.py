import polars as pl
import cdc_strategy as cdc

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
        detect_by=hash_columns,
        order_by="_dlt_load_id",
        descending=True,
        cdc_action_label=cdc_action_label,
        cdc_hash_label=cdc_hash_label
    ) 
    
    # Assert the expected CDC actions
    actual_values = result_df.select(cdc_action_label).to_series().to_list()   
    assert actual_values == ["INSERT", "UPDATE", "INSERT", "DELETE"]