/* Dimension of all the spell schools in Hearthstone */
MODEL (
  name gold.mart__cards.dim__spell_schools,
  kind VIEW
);

SELECT
  *
FROM gold.common.common_dim__spell_schools;

@export_to_parquet(@this_model, "data")