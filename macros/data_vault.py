import typing as t

from sqlmesh import macro
from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator

@macro()
def data_vault__staging(
    evaluator: MacroEvaluator,
    source: exp.Table,
    lookup_data: exp.Tuple,
    derived_columns: exp.Tuple,
    hashes: exp.Tuple,
    source_system: exp.Literal,
    loaded_at: exp.Column,
    valid_from: exp.Column | None,
    valid_to: exp.Column | None,
    hashing_type: str = "MD5"
) -> exp.Query | None:
    
    # Source CTE
    source_cte = exp.Select().select(exp.Star(table=source)).from_(source)
    
    # Lookup data CTE
    lookup_data_cte = exp.Select().select(
        exp.Column(this="*", table="source")
    ).from_("source")
    
    for lookup in lookup_data.expressions:
        lookup_column = lookup.expression.expressions[0].expression
        lookup_table = exp.Table(this=lookup.expression.expressions[1].expression.this)
        left_column = lookup.expression.expressions[2].expression
        right_column = lookup.expression.expressions[3].expression
        
        # Add column alias to the SELECT
        lookup_data_cte = lookup_data_cte.select(
            exp.Column(
                this=lookup_column.name,
                table=lookup_table.name
            ).as_(lookup.name)
        )

        # Add the JOIN
        join_condition = exp.EQ(
            this=exp.Column(this=left_column.name, table="source"),
            expression=exp.Column(this=right_column.name, table=lookup_table.name),
        )
        if valid_from and valid_to:
            join_condition = join_condition.and_(
                exp.Between(
                    this=exp.Column(this=valid_from.name, table="source"),
                    low=exp.Column(this=valid_from.name, table=lookup_table.name),
                    high=exp.Column(this=valid_to.name, table=lookup_table.name),
                )
            )

        lookup_data_cte = lookup_data_cte.join(
            exp.Join(this=lookup_table, on=join_condition, kind="LEFT")
        )       
    
    # Derived column CTE
    derived_columns_cte = exp.Select().select(
        exp.Column(this="*", table="lookup_data")
    ).from_("lookup_data")
    
    for derived_column in derived_columns.expressions:
        derived_columns_cte = derived_columns_cte.select(derived_column.expression.as_(derived_column.name))

    # Hashes CTE
    hashes_cte = exp.Select().select(
        exp.Column(this="*", table="derived_columns")
    ).from_("derived_columns")
    
    for hash in hashes.expressions:
        fields_to_hash = hash.expression
            
        if isinstance(fields_to_hash, exp.Column):
            fields_to_hash = exp.Tuple(expressions=[fields_to_hash])
        
        fields_to_concat: t.List[exp.Expression] = []

        for i, field in enumerate(fields_to_hash):
            if i > 0:
                fields_to_concat.append(exp.Literal.string("|"))
                
            fields_to_concat.append(
                exp.func(
                    "COALESCE",
                    exp.cast(field, exp.DataType.build("text")),
                    exp.Literal.string("_sqlmesh_surrogate_key_null_"),
                )
            )
        
        data_to_hash = fields_to_concat
        
        if len(data_to_hash) > 1:
            data_to_hash = exp.func("CONCAT", *data_to_hash)
        else:
            data_to_hash = data_to_hash[0]
    
        # Perform hashing
        hashed_data = exp.cast(
            exp.func(hashing_type, data_to_hash),
            exp.DataType.build("binary")
        )
        
        hashes_cte = hashes_cte.select(hashed_data.as_(hash.name))

    # Stitch together the final query
    final_query = (
        exp.Select()
        .with_("source", as_=source_cte)
        .with_("lookup_data", as_=lookup_data_cte)
        .with_("derived_columns", as_=derived_columns_cte)
        .with_("hashes", as_=hashes_cte)
        .select("*")
        .from_("hashes")
    )

    return final_query