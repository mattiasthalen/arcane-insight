# Arcane Insight
<img src="https://blz-contentstack-images.akamaized.net/v3/assets/bltc965041283bac56c/bltce748775e32f8c04/5f0769f35d2ae808119fb2dd/homepage_logo.png" height="50" alt="Hearthstone Logo" style="vertical-align: middle"> <img src="https://cdn.sanity.io/images/nsq559ov/production/7f85e56e715b847c5519848b7198db73f793448d-82x25.svg?w=2000&auto=format" height="50" alt="dltHub" style="vertical-align: middle"> <img src="https://github.com/TobikoData/sqlmesh/blob/main/docs/readme/sqlmesh.png?raw=true" height="50" alt="SQLmesh" style="vertical-align: middle"> <img src="https://duckdb.org/images/logo-dl/DuckDB_Logo-horizontal.svg" height="50" alt="DuckDB" style="vertical-align: middle">

Arcane Insight is a data analytics project designed to harness the power of SQLMesh & DuckDB to collect, transform, and analyze data from [Blizzard's Hearthstone API](https://develop.battle.net/documentation/hearthstone).

Focused on card statistics and attributes, this project reveals detailed insights into card mechanics, strengths, and trends to support BI and strategic analysis.

## Diagrams
### Architecture
```mermaid
architecture-beta
    service battle_net(cloud)[Battle Net API]

    group duckdb(disk)[DuckDB]
        group dlt(server)[dltHub] in duckdb
            group bronze(disk)[Bronze] in dlt
                service raw(database)[Raw] in bronze

        group sqlmesh(server)[SQLMesh] in duckdb
            group bronze__sql(disk)[Bronze] in sqlmesh
                service snapshot(database)[Snapshot] in bronze__sql

            group silver(disk)[Silver] in sqlmesh
                service hook(database)[Hook] in silver

            group gold(disk)[Gold] in sqlmesh
                service marts(database)[Marts] in gold

    service bi(internet)[BI]

    battle_net:R --> L:raw
    raw:R --> L:snapshot
    snapshot:R --> L:hook
    hook:R --> L:marts
    marts:R --> L:bi
```

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

### Silver
#### silver.hook.*
```mermaid
graph LR
    %% Concepts & Hooks
    subgraph concept__card["Card"]
        hook__card__id(["hook__card__id"])
        hook__card__slug(["hook__card__slug"])

        hook__card__id__copy(["hook__card__id__copy"])
        hook__card__id__parent(["hook__card__id__parent"])
    end

    subgraph concept__card_set["Card Set"]
        hook__card_set__id(["hook__card_set__id"])
        hook__card_set__slug(["hook__card_set__slug"])
    end

    subgraph concept__card_type["Card Type"]
        hook__card_type__id(["hook__card_type__id"])
        hook__card_type__slug(["hook__card_type__slug"])
    end

    subgraph concept__class["Class"]
        hook__class__id(["hook__class__id"])
        hook__class__slug(["hook__class__slug"])

        hook__class__id__tourist(["hook__class__id__tourist"])
    end

    subgraph concept__minion_type["Minion Type"]
        hook__minion_type__id(["hook__minion_type__id"])
        hook__minion_type__slug(["hook__minion_type__slug"])
    end

    subgraph concept__spell_school["Spell School"]
        hook__spell_school__id(["hook__spell_school__id"])
        hook__spell_school__slug(["hook__spell_school__slug"])
    end 

    %% Bags
    subgraph bags["Bags"]
        cards[(bag__hearthstone__cards)]
        card_sets[(bag__hearthstone__card_sets)]
        card_types[(bag__hearthstone__card_types)]
        classes[(bag__hearthstone__classes)]
        minion_types[(bag__hearthstone__minion_types)]
        spell_schools[(bag__hearthstone__spell_schools)]
    end

    %% Connections
    hook__card__id --> cards
    hook__card__slug --> cards
    hook__card__id__copy --> cards
    hook__card__id__parent --> cards

    hook__card_set__id --> card_sets
    hook__card_set__slug --> card_sets
    hook__card_set__id --> cards

    hook__card_type__id --> card_types
    hook__card_type__slug --> card_types
    hook__card_type__id --> cards

    hook__class__id --> classes
    hook__class__slug --> classes
    hook__class__id --> cards
    hook__class__id__tourist --> cards

    hook__minion_type__id --> minion_types
    hook__minion_type__slug --> minion_types
    hook__minion_type__id --> cards

    hook__spell_school__id --> spell_schools
    hook__spell_school__slug --> spell_schools
    hook__spell_school__id --> cards
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
