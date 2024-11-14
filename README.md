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
        string armor
        string artistName
        string attack
        string bannedFromSideboard
        string cardSetId FK
        string cardTypeId FK
        string childIds FK
        string classId FK
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
        string keywordIds FK
        string manaCost
        string maxSideboardCards
        string minionTypeId FK
        string multiClassIds FK
        string multiTypeIds FK
        string name
        string parentId
        string rarityId FK
        string runeCost
        string slug
        string spellSchoolId
        string text
        string touristClassId
    }

    raw__hearthstone__classes {
        string id PK
        string slug
        string name
        string cardId FK
        string heroPowerCardId FK
        string alternateHeroCardIds FK
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
        string year
        string svg
        string cardSets FK
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