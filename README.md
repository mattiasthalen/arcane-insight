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
```

### Gold
#### mart__cards
```mermaid
erDiagram
    fact__cards }o--|| dim__cards: "card_pit_hk > card_pit_hk"
    fact__cards }o--|| dim__classes: "class_pit_hk > class_pit_hk"
    
    fact__cards ||--o{ link__related_cards: "fact_record_hk > fact_record_hk"
    link__related_cards }o--|| dim__related_cards: "related_card_pit_hk > related_card_pit_hk"
```