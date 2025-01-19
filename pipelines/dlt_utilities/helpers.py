import polars as pl

def load_schema_to_df(schema: pl.Schema) -> pl.DataFrame:
    schema_df = pl.DataFrame({
        "column": list(schema.keys()),
        "dtype": [str(dtype) for dtype in schema.values()]
    })
    
    return schema_df

def print_schema(schema: pl.Schema) -> None:
    schema_df = load_schema_to_df(schema)
    
    with pl.Config() as cfg:
        cfg.set_tbl_rows(-1)
        cfg.set_tbl_cols(-1)
        
        print(schema_df)