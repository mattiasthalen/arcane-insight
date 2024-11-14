/* Dimension of all the related cards in Hearthstone */
MODEL (
  name gold.mart__cards.dim__related_cards,
  kind VIEW
);

SELECT
    @star(common_dim__cards, prefix := 'related_')
FROM gold.common.common_dim__cards;

@export_to_parquet(@this_model, "data")