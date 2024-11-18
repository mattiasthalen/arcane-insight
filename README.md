# Arcane Insight
<img src="https://blz-contentstack-images.akamaized.net/v3/assets/bltc965041283bac56c/bltce748775e32f8c04/5f0769f35d2ae808119fb2dd/homepage_logo.png" height="50" alt="Hearthstone Logo" style="vertical-align: middle"> <img src="https://github.com/TobikoData/sqlmesh/blob/main/docs/readme/sqlmesh.png?raw=true" height="50" alt="SQLmesh" style="vertical-align: middle"> <img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-horizontal.svg" height="50" alt="DuckDB" style="vertical-align: middle">

Arcane Insight is a data analytics project designed to harness the power of SQLMesh & DuckDB to collect, transform, and analyze data from [Blizzard's Hearthstone API](https://develop.battle.net/documentation/hearthstone).

Focused on card statistics and attributes, this project reveals detailed insights into card mechanics, strengths, and trends to support BI and strategic analysis.

## ERDs
### Bronze
#### bronze.raw.*
```mermaid
erDiagram
    raw__hearthstone__bg_game_modes
    raw__hearthstone__cardback_categories
    
    raw__hearthstone__cards }|--|| raw__hearthstone__cards: "parentId > id"
    raw__hearthstone__cards }|--|| raw__hearthstone__cards: "childIds > id"
    raw__hearthstone__cards||--|| raw__hearthstone__cards: "copyOfCardId > id"
    
    raw__hearthstone__cards||--|{ raw__hearthstone__cardbacks: "id > id"
    raw__hearthstone__cards||--|{ raw__hearthstone__classes: "classId > id"
    raw__hearthstone__cards }|--|{ raw__hearthstone__classes: "multiClassIds > id"
    raw__hearthstone__cards }|--|{ raw__hearthstone__keywords: "keywordIds > id"
    raw__hearthstone__cards||--|{ raw__hearthstone__minion_types: "minionTypeId > id"
    raw__hearthstone__cards||--|{ raw__hearthstone__rarities: "rarityId > id"
    raw__hearthstone__cards||--|{ raw__hearthstone__sets: "cardSetId > id"
    raw__hearthstone__cards||--|{ raw__hearthstone__types: "cardTypeId > id"
    raw__hearthstone__cards }|--|{ raw__hearthstone__types: "multiTypeIds > id"
    raw__hearthstone__cards ||--|{ raw__hearthstone__spell_schools: "multiTypeIds > id"
    
    raw__hearthstone__classes }|--|| raw__hearthstone__cards: "cardId > id"
    raw__hearthstone__classes }|--|| raw__hearthstone__cards: "heroPowerCardId > id"
    raw__hearthstone__classes }|--|{ raw__hearthstone__cards: "alternateHeroCardIds > id"
    
    raw__hearthstone__keywords ||--|{ raw__hearthstone__game_modes: "gameModes > id"
    raw__hearthstone__mercenary_factions
    raw__hearthstone__mercenary_roles
    raw__hearthstone__minion_types ||--|{ raw__hearthstone__game_modes: "gameModes > id"
    raw__hearthstone__sets }|--|{ raw__hearthstone__set_groups: "id > cardSets"
    raw__hearthstone__spell_schools ||--|{ raw__hearthstone__game_modes: "gameModes > id"
    raw__hearthstone__types ||--|{ raw__hearthstone__game_modes: "gameModes > id"
```

### Gold
#### gold.mart__cards.*
```mermaid
erDiagram
    fact__cards }|--|| dim__cardbacks: "cardback_pit_hk"
    fact__cards }|--|| dim__cards: "card_pit_hk"
    fact__cards }|--|| dim__classes: "class_pit_hk"
    fact__cards }|--|| dim__minion_types: "minion_type_pit_hk"
    fact__cards }|--|| dim__rarities: "rarity_pit_hk"
    fact__cards }|--|| dim__tourist_classes: "tourist_class_pit_hk"
    fact__cards }|--|| dim__types: "type_pit_hk"
    fact__cards }|--|| dim__sets: "set_pit_hk"
    
    fact__cards ||--|{ link__related_cards: "fact_record_hk"
    link__related_cards }|--|| dim__related_cards: "related_card_pit_hk"
    
    fact__cards ||--|{ link__related_classes: "fact_record_hk"
    link__related_classes }|--|| dim__related_classes: "related_class_pit_hk"
    
    fact__cards ||--|{ link__related_types: "fact_record_hk"
    link__related_types }|--|| dim__related_types: "related_type_pit_hk"
    
    fact__cards ||--|{ link__keywords: "fact_record_hk"
    link__keywords }|--|| dim__keywords: "keyword_pit_hk"
```