import os
import typing as t

from sqlmesh import macro
from sqlglot import exp

@macro()
def export_to_parquet(evaluator, table: exp.Table, path: str) -> str | None:
    if evaluator.runtime_stage != 'evaluating':
        return None
    
    catalog_name, schema_name, table_name = [f"{part}".strip('"') for part in table.parts]
    filename = table_name.rsplit('__', 1)[0] + ".parquet"
    file_path = os.path.join(path, catalog_name, schema_name, filename)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    sql = f"COPY {table} TO '{file_path}' (FORMAT 'parquet')"
    
    return sql