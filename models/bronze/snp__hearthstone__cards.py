import pandas as pd
import typing as t

from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

@model(
    name="bronze.snp__hearthstone__cards",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key="hash_diff"
    ),
    columns={
         "id": "bigint"
    ,    "collectible": "bigint"
    ,    "slug": "text"
    ,    "class_id": "bigint"
    ,    "spell_school_id": "bigint"
    ,    "card_type_id": "bigint"
    ,    "card_set_id": "bigint"
    ,    "rarity_id": "bigint"
    ,    "artist_name": "text"
    ,    "mana_cost": "bigint"
    ,    "name": "text"
    ,    "text": "text"
    ,    "image": "text"
    ,    "image_gold": "text"
    ,    "flavor_text": "text"
    ,    "crop_image": "text"
    ,    "is_zilliax_functional_module": "bool"
    ,    "is_zilliax_cosmetic_module": "bool"
    ,    "_dlt_load_id": "text"
    ,    "_dlt_id": "text"
    ,    "health": "bigint"
    ,    "attack": "bigint"
    ,    "minion_type_id": "bigint"
    ,    "rune_cost__blood": "bigint"
    ,    "rune_cost__frost": "bigint"
    ,    "rune_cost__unholy": "bigint"
    ,    "armor": "bigint"
    ,    "durability": "bigint"
    ,    "parent_id": "bigint"
    ,    "banned_from_sideboard": "bigint"
    ,    "tourist_class_id": "bigint"
    ,    "copy_of_card_id": "bigint"
    ,    "max_sideboard_cards": "bigint"
    ,    "_sqlmesh_hash_diff": "text"
    ,    "_sqlmesh_loaded_at": "datetime"
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    
    source_df = context.spark.read.parquet("./warehouse/bronze/battle_net/raw_hearthstone_cards/1731050241.346379.ffc7685335.parquet")
    source_df = source_df.withColumn('_dlt_loaded_at', F.from_unixtime(F.col('_dlt_load_id').cast('double')))
    
    source_df.createOrReplaceTempView("raw__hearthstone__cards")
    
    sql_query = f"""
        SELECT *, 
               HEX(SHA2(CONCAT_WS('|', {', '.join([f'`{col}`' for col in source_df.columns if not col.startswith('_dlt')])}), 256)) AS _sqlmesh_hash_diff,
               '{execution_time}' AS _sqlmesh_loaded_at
        FROM raw__hearthstone__cards
        WHERE _dlt_loaded_at >= '{start}' AND _dlt_loaded_at <= '{end}'
    """
    
    final_df = context.spark.sql(sql_query)
    
    return final_df