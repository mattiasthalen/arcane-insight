import os
import typing as t

from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator
from sqlglot import exp

@macro()
def data_vault__staging(
    evaluator: MacroEvaluator,
    source: exp.Table,
    business_keys: t.List[exp.Expression],
    source_system: exp.Literal,
    loaded_at: exp.Column,
    valid_from: exp.Column | None,
    valid_to: exp.Column | None,
) -> exp.Expression:
    
    source_name = exp.Literal.string(source)
    
    source_expression = (
        exp.select("*")
        .select(source_system.as_("_sqlmesh__source_system"))
        .select(source_name.as_("_sqlmesh__source_table"))
        .from_(source)
    )
    
    final_expression = source_expression
    return final_expression