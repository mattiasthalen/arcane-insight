from sqlmesh import macro
from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator

@macro()
def data_vault__staging(
    evaluator: MacroEvaluator,
    source: exp.Table,
    lookup_data: exp.Tuple,
    business_keys: exp.Tuple,
    hashes: exp.Tuple,
    source_system: exp.Literal,
    loaded_at: exp.Column,
    valid_from: exp.Column | None,
    valid_to: exp.Column | None,
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
    
    # Business keys CTE
    business_keys_cte = exp.Select().select(
        exp.Column(this="*", table="lookup_data")
    ).from_("lookup_data")
    
    for business_key in business_keys.expressions:
        business_keys_cte = business_keys_cte.select(business_key.expression.as_(business_key.name))

    # Hashes CTE
    hashes_cte = exp.Select().select(
        exp.Column(this="*", table="business_keys")
    ).from_("business_keys")
    
    for hash in hashes.expressions:
        print(type(hash.expression))

    # Stitch together the final query
    final_query = (
        exp.Select()
        .with_("source", as_=source_cte)
        .with_("lookup_data", as_=lookup_data_cte)
        .with_("business_keys", as_=business_keys_cte)
        .with_("hashes", as_=hashes_cte)
        .select("*")
        .from_("hashes")
    )

    return None