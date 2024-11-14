# Arcane Insight
Arcane Insight is a data analytics project designed to harness the power of SQLMesh & DuckDB to collect, transform, and analyze data from Blizzard’s Hearthstone API.
Focused on card statistics and attributes, this project reveals detailed insights into card mechanics, strengths, and trends to support BI and strategic analysis.

## ERDs
### Bronze
```mermaid
erDiagram
    raw__hearthstone__cards }o--|| raw__hearthstone__cards: "parentId > id"
    raw__hearthstone__cards }o--|| raw__hearthstone__cards: "childIds > id"
    raw__hearthstone__cards||--|| raw__hearthstone__cards: "copyOfCardId > id"
    
    raw__hearthstone__cards||--o{ raw__hearthstone__cardbacks: "id > id"
    raw__hearthstone__cards||--o{ raw__hearthstone__classes: "classId > id"
    raw__hearthstone__cards }o--o{ raw__hearthstone__classes: "multiClassIds > id"
    raw__hearthstone__cards }o--o{ raw__hearthstone__keywords: "keywordIds > id"
    raw__hearthstone__cards||--o{ raw__hearthstone__minion_types: "minionTypeId > id"
    raw__hearthstone__cards||--o{ raw__hearthstone__rarities: "rarityId > id"
    raw__hearthstone__cards||--o{ raw__hearthstone__sets: "cardSetId > id"
    raw__hearthstone__cards||--o{ raw__hearthstone__types: "cardTypeId > id"
    raw__hearthstone__cards }o--o{ raw__hearthstone__types: "multiTypeIds > id"
    raw__hearthstone__sets }o--o{ raw__hearthstone__set_groups: "id > cardSets"
    
    raw__hearthstone__classes }o--|| raw__hearthstone__cards: "cardId > id"
    raw__hearthstone__classes }o--|| raw__hearthstone__cards: "heroPowerCardId > id"
    raw__hearthstone__classes }o--o{ raw__hearthstone__cards: "alternateHeroCardIds > id"

    raw__hearthstone__cardbacks {
        string id PK
        string cardbackCategory
        string text
        string name
        string sortCategory
        string slug
        string image
    }

    raw__hearthstone__cards {
        string id PK
        string cardSetId FK
        string cardTypeId FK
        string childIds FK
        string classId FK
        string keywordIds FK
        string minionTypeId FK
        string multiClassIds FK
        string multiTypeIds FK
        string rarityId FK
        string armor
        string artistName
        string attack
        string bannedFromSideboard
        string collectible
        string copyOfCardId
        string cropImage
        string durability
        string flavorText
        string health
        string image
        string imageGold
        string isZilliaxCosmeticModule
        string isZilliaxFunctionalModule
        string manaCost
        string maxSideboardCards
        string name
        string parentId
        string runeCost
        string slug
        string spellSchoolId
        string text
        string touristClassId
    }

    raw__hearthstone__classes {
        string id PK
        string cardId FK
        string heroPowerCardId FK
        string alternateHeroCardIds FK
        string slug
        string name
    }

    raw__hearthstone__keywords {
        string id PK
        string slug
        string name
        string refText
        string text
        string gameModes
    }

    raw__hearthstone__minion_types {
        string id PK
        string slug
        string name
        string gameModes
    }

    raw__hearthstone__rarities {
        string id PK
        string slug
        string name
        string craftingCost
        string dustValue
    }

    raw__hearthstone__set_groups {
        string slug PK
        string cardSets FK
        string year
        string svg
        string name
        string standard
        string icon
        string yearRange
    }

    raw__hearthstone__sets {
        string id PK
        string name
        string slug
        string hyped
        string type
        string collectibleCount
        string collectibleRevealedCount
        string nonCollectibleCount
        string nonCollectibleRevealedCount
        string aliasSetIds
    }

    raw__hearthstone__types {
        string id PK
        string slug
        string name
        string gameModes
    }
```

### Gold
#### mart__cards
```mermaid
erDiagram
    fact__cards }o--|| dim__cards: "card_pit_hk > card_pit_hk"
    fact__cards }o--|| dim__classes: "class_pit_hk > class_pit_hk"
    
    fact__cards ||--o{ link__related_cards: "fact_record_hk > fact_record_hk"
    link__related_cards }o--|| dim__related_cards: "related_card_pit_hk > related_card_pit_hk"
    

    fact__cards {
        varbinary fact_record_hk PK
        varbinary card_relations_hk FK
        int card_set_id FK
        int card_type_id FK
        int class_id FK
        varbinary class_pit_hk FK
        array keyword_ids FK
        int minion_type_id FK
        array multi_class_ids FK
        array multi_type_ids FK
        int rarity_id FK
        int spell_school_id FK
        int tourist_class_id FK
        string fact_name
        struct card_relations
        boolean is_zilliax_cosmetic_module
        boolean is_zilliax_functional_module
        int mana_cost
        int blood_rune_cost
        int frost_rune_cost
        int unholy_rune_cost
        int total_rune_cost
        timestamp fact__extracted_at
        timestamp fact__loaded_at
        int fact__version
        timestamp fact__valid_from
        timestamp fact__valid_to
        boolean fact__is_current_record
    }
    
    dim__cards {
        varbinary card_pit_hk PK
        int card_id
        int card__armor
        string card__artist_name
        int card__attack
        boolean card__banned_from_sideboard
        boolean card__collectible
        string card__crop_image
        string card__durability
        string card__flavor_text
        int card__health
        string card__image
        string card__image_gold
        string card__name
        string card__slug
        string card__text
        timestamp card__extracted_at
        timestamp card__loaded_at
        varbinary card__hash_diff
        int card__version
        timestamp card__valid_from
        timestamp card__valid_to
        boolean card__is_current_record
    }
    
    link__related_cards {
        varbinary fact_record_hk FK
        varbinary related_card_pit_hk FK
        text card_relation
        timestamp link__extracted_at
        timestamp link__loaded_at
        timestamp link__valid_from
        timestamp link__valid_to
    }
    
    dim__related_cards {
        varbinary related_card_pit_hk PK
        int related_card_id
        int related_card__armor
        string related_card__artist_name
        int related_card__attack
        boolean related_card__banned_from_sideboard
        boolean related_card__collectible
        string related_card__crop_image
        string related_card__durability
        string related_card__flavor_text
        int related_card__health
        string related_card__image
        string related_card__image_gold
        string related_card__name
        string related_card__slug
        string related_card__text
        timestamp related_card__extracted_at
        timestamp related_card__loaded_at
        varbinary related_card__hash_diff
        int related_card__version
        timestamp related_card__valid_from
        timestamp related_card__valid_to
        boolean related_card__is_current_record
    }
    
    dim__classes {
        varbinary class_pit_hk
        int class_id
        text class_slug
        text class_name
        timestamp class__extracted_at
        timestamp class__loaded_at
        varbinary class__hash_diff
        int class__version
        timestamp class__valid_from
        timestamp class__valid_to
        boolean class__is_current_record
    }
```