import typing as t

from pandas.core.window import common
from sqlglot.expressions import Subquery, Timestamp
from sqlmesh import macro
from sqlglot import exp
from sqlmesh.core.macros import MacroEvaluator

@macro()
def data_vault__staging(
    evaluator: MacroEvaluator,
    source: exp.Table,
    source_system: exp.Literal,
    loaded_at: exp.Column,
    lookup_data: exp.Tuple | None = None,
    derived_columns: exp.Tuple | None = None,
    hashes: exp.Tuple | None = None,
    valid_from: exp.Column | None = None,
    valid_to: exp.Column | None = None,
    generate_ghost_record: bool = True,
    hash_function: exp.Literal = exp.Literal.string("MD5")
) -> exp.Query:
    
    # Define final query
    sql = exp.Select().select(exp.Star())
    
    # Source CTE
    source_cte = (
        exp.Select()
        .select(
            exp.Star(),
            source_system.as_("source_system"),
            exp.Literal.string(source).as_("source_table")
        )
        .from_(source)
    )
    sql = sql.with_("source", as_=source_cte)
    previous_table = exp.Table(this="source")
    
    # Lookup data CTE
    if lookup_data:
        lookup_data_cte = exp.Select().select(
            exp.Column(this=exp.Star(), table=previous_table)
        ).from_(previous_table)
        
        for lookup in lookup_data.expressions:

            lookup_table = exp.Table(
                this=lookup.expression.expressions[0].expression.this,
                db=lookup.expression.expressions[0].expression.table,
                catalog=lookup.expression.expressions[0].expression.db,
            )
            
            lookup_column = exp.Column(
                this=lookup.expression.expressions[1].expression.this,
                table=lookup_table
            )
            
            left_column = exp.Column(
                this=lookup.expression.expressions[2].expression.this,
                table=previous_table
            )
            
            right_column = exp.Column(
                this=lookup.expression.expressions[3].expression.this,
                table=lookup_table
            )
            
            # Add column alias to the SELECT
            lookup_data_cte = lookup_data_cte.select(lookup_column.as_(lookup.name)
            )
    
            # Add the JOIN
            join_condition = exp.EQ(
                this=left_column,
                expression=right_column
            )
            if valid_from and valid_to:
                left_valid_from = exp.Column(
                    this=valid_from.this,
                    table=previous_table
                )
                
                right_valid_from = exp.Column(
                    this=valid_from.this,
                    table=lookup_table
                )
                
                right_valid_to = exp.Column(
                    this=valid_to.this,
                    table=lookup_table
                )
                
                join_condition = join_condition.and_(
                    exp.Between(
                        this=left_valid_from,
                        low=right_valid_from,
                        high=right_valid_to
                    )
                )
    
            lookup_data_cte = lookup_data_cte.join(
                exp.Join(this=lookup_table, on=join_condition, kind="LEFT")
            )  
        
        sql = sql.with_("lookup_data", as_=lookup_data_cte)
        previous_table = exp.Table(this="lookup_data")

    # Derived column CTE
    if derived_columns:
        derived_columns_cte = exp.Select().select(exp.Star()).from_(previous_table)
        
        for derived_column in derived_columns.expressions:
            derived_columns_cte = derived_columns_cte.select(derived_column.expression.as_(derived_column.name))
        
        sql = sql.with_("derived_columns", as_=derived_columns_cte)
        previous_table = exp.Table(this="derived_columns")
        
    # Ghost record CTE
    if generate_ghost_record:
        replace_fields = ', '.join([f"ghost_record_subquery.{col} as {col}" for col in ["source_system", "source_table", loaded_at.name, valid_from.name, valid_to.name] if col])

        ghost_record_cte = exp.Select().select(
            f"{previous_table}.* REPLACE ({replace_fields})"
        )
        
        ghost_record_subquery = (
            exp.Select()
            .select(
                exp.Literal.string("ghost_record").as_("source_system"),
                exp.Literal.string("ghost_record").as_("source_table"),
                exp.Cast(
                    this=exp.Literal.string("0001-01-01 00:00:00"),
                    to=exp.DataType.build("TIMESTAMP")
                ).as_(loaded_at.name)
            )
        )
        
        if valid_from and valid_to:
            ghost_record_subquery = ghost_record_subquery.select(
                exp.Cast(
                    this=exp.Literal.string("0001-01-01 00:00:00"),
                    to=exp.DataType.build("TIMESTAMP")
                ).as_(valid_from.name),
                exp.Cast(
                    this=exp.Literal.string("9999-12-31 23:59:59"),
                    to=exp.DataType.build("TIMESTAMP")
                ).as_(valid_to.name)
            )
        
        ghost_record_cte = ghost_record_cte.from_(ghost_record_subquery.subquery("ghost_record_subquery"))
        
        join_conditions = [
            exp.EQ(
                this=exp.Column(this="source_system", table="ghost_record_subquery"),
                expression=exp.Column(this="source_system", table=previous_table)
            ),
            exp.EQ(
                this=exp.Column(this="source_table", table="ghost_record_subquery"),
                    expression=exp.Column(this="source_table", table=previous_table)
                ),
            exp.EQ(
                this=exp.Column(this=loaded_at.name, table="ghost_record_subquery"),
                expression=exp.Column(this=loaded_at.name, table=previous_table)
            )
        ]
        
        if valid_from and valid_to:
            join_conditions.extend([
                exp.EQ(
                    this=exp.Column(this=valid_from.name, table="ghost_record_subquery"),
                    expression=exp.Column(this=valid_from.name, table=previous_table)
                ),
                exp.EQ(
                    this=exp.Column(this=valid_to.name, table="ghost_record_subquery"),
                    expression=exp.Column(this=valid_to.name, table=previous_table)
                )
            ])
        
        ghost_record_join_condition = exp.and_(*join_conditions)
        
        ghost_record_cte = ghost_record_cte.join(
            exp.Join(
                this=previous_table,
                on=ghost_record_join_condition,
                kind="LEFT"
            )
        )
            
        ghost_record_cte = ghost_record_cte.union(
            exp.Select().select(exp.Star()).from_(previous_table)
        )
        
        sql = sql.with_("ghost_record", as_=ghost_record_cte)
        previous_table = exp.Table(this="ghost_record")
    
    # Hashes CTE
    if hashes:
        hashes_cte = exp.Select().select(exp.Star()).from_(previous_table)
        
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
    
            concatenated_data = exp.func("CONCAT", *fields_to_concat)
        
            # Perform hashing
            hashed_data = exp.cast(
                exp.func(hash_function.name, concatenated_data),
                exp.DataType.build("binary")
            )
            
            hashes_cte = hashes_cte.select(hashed_data.as_(hash.name))
        
        sql = sql.with_("hashes", as_=hashes_cte)
        previous_table = exp.Table(this="hashes")
    
    sql = sql.from_(previous_table)
    
    return sql