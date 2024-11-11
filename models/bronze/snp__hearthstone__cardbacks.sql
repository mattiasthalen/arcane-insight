MODEL (
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key _sqlmesh__hash_diff,
    forward_only FALSE,
    disable_restatement FALSE
  )
);

SELECT
    *,
    @generate_surrogate_key__sha_256(
        @star_v2(
            relation := raw__hearthstone__cardbacks,
            exclude := _sqlmesh__extracted_at,
            select_only := TRUE
        )
    ) AS _sqlmesh__hash_diff,
    @execution_ts::TIMESTAMP AS _sqlmesh__loaded_at
FROM bronze.raw__hearthstone__cardbacks