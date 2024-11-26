import typing as t

from functools import reduce
from pandas._libs.tslibs.offsets import BusinessDay
from pandas.core.window import common
from sqlglot.expressions import Subquery, Timestamp
from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

@macro()
def data_vault__staging(
    evaluator: MacroEvaluator,
    source: exp.Table,
    source_system: exp.Literal,
    loaded_at: exp.Column,
    lookup_data: exp.Array | None = None,
    derived_columns: exp.Array | None = None,
    hashes: exp.Array | None = None,
    valid_from: exp.Column | None = None,
    valid_to: exp.Column | None = None,
    generate_ghost_record: bool = True,
    hash_function: exp.Literal = exp.Literal.string("MD5")
) -> exp.Query:
    """
    Prepares a staged query for a Data Vault model, incorporating transformations like lookups,
    derived columns, ghost records, and hash calculations.

    Args:
        source (exp.Table): Source table with raw data.
        source_system (exp.Literal): Literal representing the source system.
        loaded_at (exp.Column): Timestamp column indicating the data load time.
        lookup_data (exp.Array | None): Array defining lookups to enrich the source table. Each entry includes:
            - lookup_table (exp.Table): The table to join.
            - lookup_column (exp.Column): The column to retrieve.
            - left_column (exp.Column): The column from the source table to join on.
            - right_column (exp.Column): The column from the lookup table to join on.
        derived_columns (exp.Array | None): Array defining derived columns. Each entry includes:
            - alias (exp.Column): The name to alias the derived column as.
            - expression (exp.Expression): The expression for the derived column.
        hashes (exp.Array | None): Array defining hash calculations. Each entry includes:
            - alias (exp.Column): The name of the hash column.
            - fields (exp.Array): The fields to hash together.
        valid_from (exp.Column | None): Column for the validity start timestamp.
        valid_to (exp.Column | None): Column for the validity end timestamp.
        generate_ghost_record (bool): Whether to generate a ghost record. Default is True.
        hash_function (exp.Literal): The hash function to use (e.g., MD5, SHA256). Default is MD5.

    Returns:
        exp.Query: The final staged SQL query.

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = \"\"\"
        ... @data_vault__staging(
        ...   source := bronze.snapshot.snp__hearthstone__cards,
        ...   lookup_data := [
        ...     class_bk := (
        ...       lookup_table := bronze.snapshot.snp__hearthstone__classes,
        ...       lookup_column := slug,
        ...       left_column := classId,
        ...       right_column := id
        ...     )
        ...   ],
        ...   derived_columns := [
        ...     card_bk := slug
        ...   ],
        ...   hashes := [
        ...     card_hk := card_bk,
        ...     class_hk := class_bk,
        ...     card_hk__class_hk := (card_bk, class_bk),
        ...     card_pit_hk := (card_bk, _sqlmesh__valid_from)
        ...   ],
        ...   source_system := 'hearthstone',
        ...   loaded_at := _sqlmesh__loaded_at,
        ...   valid_from := _sqlmesh__valid_from,
        ...   valid_to := _sqlmesh__valid_to,
        ...   generate_ghost_record := TRUE,
        ...   hash_function := 'SHA256'
        ... )
    """
    
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
                alias=lookup.name
            )
            
            lookup_column = exp.Column(
                this=lookup.expression.expressions[1].expression.this,
                table=lookup.name
            ).as_(lookup.name)
            
            left_column = exp.Column(
                this=lookup.expression.expressions[2].expression.this,
                table=previous_table
            )
            
            right_column = exp.Column(
                this=lookup.expression.expressions[3].expression.this,
                table=lookup.name
            )
            
            # Add column alias to the SELECT
            lookup_data_cte = lookup_data_cte.select(lookup_column)
    
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
                    table=lookup.name
                )
                
                right_valid_to = exp.Column(
                    this=valid_to.this,
                    table=lookup.name
                )
                
                join_condition = join_condition.and_(
                    exp.Between(
                        this=left_valid_from,
                        low=right_valid_from,
                        high=right_valid_to
                    )
                )
    
            lookup_data_cte = lookup_data_cte.join(
                exp.Join(
                    this=lookup_table,
                    on=join_condition,
                    kind="LEFT"
                )
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
        common_fields = ["source_system", "source_table", loaded_at.name]
        
        if valid_to and valid_from:
            common_fields.extend([valid_from.name, valid_to.name])
            
        replace_fields = ', '.join(f"ghost_record_subquery.{col} as {col}" for col in common_fields)

        ghost_record_cte = exp.Select().select(f"{previous_table}.* REPLACE ({replace_fields})")
        
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
                fields_to_hash = exp.Array(expressions=[fields_to_hash])
            
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
    
@macro()
def data_vault__hub(
    evaluator: MacroEvaluator,
    sources: exp.Array,
    business_key: exp.Column,
    source_system: exp.Column,
    source_table: exp.Column,
    loaded_at: exp.Column
) -> exp.Query:
    """Creates a Data Vault Hub entity by combining multiple source tables with priority-based deduplication.
       
    This macro implements a Data Vault hub loading pattern that handles multiple source systems
    with a priority-based approach to business key selection. Sources are evaluated in order of
    priority (first source has highest priority) when determining the authoritative version of
    a business key.
    
    Args:
       sources: Array of source configurations, where each source is defined as
               table_reference := (column_mapping)
       business_key: Column expression representing the hub's business key
       source_system: Column expression for the source system identifier
       source_table: Column expression for the source table name
       loaded_at: Column expression for the load timestamp
    
    Implementation Details:
       1. Assigns a source_id (0,1,2...) to each source based on priority order
       2. Unions all sources with their respective source_ids
       3. Deduplicates business keys by:
          - Partitioning by business key
          - Ordering by source_id (lower = higher priority) and loaded_at (earlier = preferred)
          - Selecting the first record per partition
    
    Example:
       ```sql
       @data_vault__hub(
           sources := [
               silver.data_vault.dv_stg__hearthstone__cards := (
                   card_bk,
                   card_hk
               ),
               silver.data_vault.dv_stg__hearthstone__classes := (
                   hero_power_card_bk card_bk,
                   hero_power_card_hk card_hk
               )
           ],
           business_key := card_bk,
           source_system := source_system,
           source_table := source_table,
           loaded_at := *sqlmesh*_loaded_at
       )
       ```
    
    Returns:
       exp.Query: A SQLGlot query expression that implements the hub loading logic
    """
    
    # Define sql
    sql = exp.Select().select(exp.Star())
    
    # Define union cte
    selects = []
    
    for id, source in enumerate(sources):
        from_table = exp.Table(
            this=source.this.name,
            db=source.this.table,
            catalog=source.this.db
        )
        
        source_expression = source.expression
        
        select = (
            exp.Select()
            .distinct()
            .select(
                exp.Column(
                    this=exp.Int64(this=id)
                ).as_("source_id")
            )
            .select(*source_expression)
            .select(loaded_at)
            .from_(from_table)
        )
        
        selects.append(select)
    
    union_keys_cte = reduce(lambda x, y: x.union(y, distinct=True), selects)
    sql = sql.with_("union_keys", as_=union_keys_cte)
    previous_table = exp.Table(this="union_keys")

    # Define deduplication cte
    deduplicate_cte = (
        exp.Select()
        .select(exp.Star())
        .from_(previous_table)
        .qualify(
            exp.Window(
                this=exp.RowNumber(),
                partition_by=[business_key],
                order=exp.Order(
                    expressions=[
                        exp.Ordered(this=exp.Column(this="source_id")),
                        exp.Ordered(this=exp.Column(this=loaded_at)),
                    ]
                )
            ).eq(1)
        )
    )
    
    sql = sql.with_("deduplicate", as_=deduplicate_cte)
    previous_table = exp.Table(this="deduplicate")
    
    sql = sql.from_(previous_table)
    
    return sql
    
@macro()
def data_vault__link(
    evaluator: MacroEvaluator,
    source: exp.Array,
    source_system: exp.Column,
    source_table: exp.Column,
    loaded_at: exp.Column
) -> exp.Query | None:
    pass
    
@macro()
def data_vault__sat(
    evaluator: MacroEvaluator,
    source: exp.Array,
    source_system: exp.Column,
    source_table: exp.Column,
    loaded_at: exp.Column
) -> exp.Query | None:
    pass