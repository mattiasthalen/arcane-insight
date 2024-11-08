import typing as t
from datetime import datetime

import pandas as pd
from pyspark.sql import DataFrame, functions

from sqlmesh import ExecutionContext, model

@model(
    name="bronze.snp__hearthstone__cards",
    kind="full",
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
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> DataFrame:
    
    df = context.spark.read.parquet("./warehouse/bronze/battle_net/raw_hearthstone_cards/1731050241.346379.ffc7685335.parquet")
    
    return df