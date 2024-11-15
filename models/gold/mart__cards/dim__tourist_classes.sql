/* Dimension of all the tourist classes in Hearthstone */
MODEL (
  name gold.mart__cards.dim__tourist_classes,
  kind VIEW
);

SELECT
  @star(common_dim__classes, prefix := 'tourist_')
FROM gold.common.common_dim__classes;

@export_to_parquet(@this_model, "data")