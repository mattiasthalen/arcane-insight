/* Dimension of all the minion types in Hearthstone */
MODEL (
  name gold.mart__cards.dim__minion_types,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__minion_types;

@export_to_parquet(@this_model, "data")