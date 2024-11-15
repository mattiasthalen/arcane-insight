/* Dimension of all the related types in Hearthstone */
MODEL (
  name gold.mart__cards.dim__related_types,
  kind VIEW
);

SELECT
  @star(common_dim__types, prefix := 'related_')
FROM gold.common.common_dim__types;

@export_to_parquet(@this_model, "data")