# Arcane Insight
Arcane Insight is a data analytics project designed to harness the power of SQLMesh & DuckDB to collect, transform, and analyze data from Blizzard’s Hearthstone API.
Focused on card statistics and attributes, this project reveals detailed insights into card mechanics, strengths, and trends to support BI and strategic analysis.

## ERDs
### Bronze
```mermaid
erDiagram
    bronze.raw.raw__hearthstone__cards }o--|| bronze.raw.raw__hearthstone__cards: "parentId > id"
    bronze.raw.raw__hearthstone__cards }o--|| bronze.raw.raw__hearthstone__cards: "childIds > id"
    bronze.raw.raw__hearthstone__cards||--|| bronze.raw.raw__hearthstone__cards: "copyOfCardId > id"
    
    bronze.raw.raw__hearthstone__cards||--o{ bronze.raw.raw__hearthstone__cardbacks: "id > id"
    bronze.raw.raw__hearthstone__cards||--o{ bronze.raw.raw__hearthstone__classes: "classId > id"
    bronze.raw.raw__hearthstone__cards }o--o{ bronze.raw.raw__hearthstone__classes: "multiClassIds > id"
    bronze.raw.raw__hearthstone__cards }o--o{ bronze.raw.raw__hearthstone__keywords: "keywordIds > id"
    bronze.raw.raw__hearthstone__cards||--o{ bronze.raw.raw__hearthstone__minion_types: "minionTypeId > id"
    bronze.raw.raw__hearthstone__cards||--o{ bronze.raw.raw__hearthstone__rarities: "rarityId > id"
    bronze.raw.raw__hearthstone__cards||--o{ bronze.raw.raw__hearthstone__sets: "cardSetId > id"
    bronze.raw.raw__hearthstone__cards||--o{ bronze.raw.raw__hearthstone__types: "cardTypeId > id"
    bronze.raw.raw__hearthstone__cards }o--o{ bronze.raw.raw__hearthstone__types: "multiTypeIds > id"
    bronze.raw.raw__hearthstone__sets }o--o{ bronze.raw.raw__hearthstone__set_groups: "id > cardSets"
    
    bronze.raw.raw__hearthstone__classes }o--|| bronze.raw.raw__hearthstone__cards: "cardId > id"
    bronze.raw.raw__hearthstone__classes }o--|| bronze.raw.raw__hearthstone__cards: "heroPowerCardId > id"
    bronze.raw.raw__hearthstone__classes }o--o{ bronze.raw.raw__hearthstone__cards: "alternateHeroCardIds > id"

    bronze.raw.raw__hearthstone__cardbacks {
        string id PK
        string cardbackCategory
        string text
        string name
        string sortCategory
        string slug
        string image
    }

    bronze.raw.raw__hearthstone__cards {
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

    bronze.raw.raw__hearthstone__classes {
        string id PK
        string slug
        string name
        string cardId FK
        string heroPowerCardId FK
        string alternateHeroCardIds FK
    }

    bronze.raw.raw__hearthstone__keywords {
        string id PK
        string slug
        string name
        string refText
        string text
        string gameModes
    }

    bronze.raw.raw__hearthstone__minion_types {
        string id PK
        string slug
        string name
        string gameModes
    }

    bronze.raw.raw__hearthstone__rarities {
        string id PK
        string slug
        string name
        string craftingCost
        string dustValue
    }

    bronze.raw.raw__hearthstone__set_groups {
        string slug PK
        string year
        string svg
        string cardSets FK
        string name
        string standard
        string icon
        string yearRange
    }

    bronze.raw.raw__hearthstone__sets {
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

    bronze.raw.raw__hearthstone__types {
        string id PK
        string slug
        string name
        string gameModes
    }
```