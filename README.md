# StatCan Data Lakehouse ELT Pipeline

A local data engineering pipeline that cleans Statistics Canada census data, stores it in a PostgreSQL Snowflake Schema, and converts it to compressed Parquet files for fast SQL queries in DuckDB. This architecture mimics how Amazon Redshift, S3, and Amazon Athena work together in the cloud.

---

## Technical Stack

* **Languages:** Python 3.13, SQL
* **Relational Database:** PostgreSQL 15+ (Staging Warehouse & Snowflake Schema)
* **Analytical Engine:** DuckDB (Fast SQL queries directly on Parquet files)
* **Data Libraries:** Pandas, SQLAlchemy, `requests`
* **Environment:** Docker, WSL 2 (Ubuntu Linux)

---

## Project Overview

This project builds a complete data pipeline using national demographic and economic census data from Statistics Canada (Product ID 14100287). 

While it runs locally on a laptop, it is built to mirror an **AWS Cloud Data Lakehouse** setup:
* **PostgreSQL** Serves as the central analytical warehouse, modeling the dimensional data structure of **Amazon Redshift**.
* **DuckDB** Runs SQL queries directly on local Parquet files, mimicking serverless **Amazon Athena** (or **Redshift Spectrum**) queries over S3 storage.

---

## Pipeline Architecture

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#F8FAFC' }}}%%
flowchart TD
    A("StatCan API<br/>Zip Archive Endpoint")
    B("Python BytesIO<br/>In-Memory Buffer")
    C("Pandas Parser<br/>50,000 Row Chunks")
    D[("PostgreSQL<br/>staging_ethnic_origins")]

    E("StatCan Metadata API<br/>POST /getCubeMetadata")
    F("SQLAlchemy<br/>ON CONFLICT Upsert")
    G[("PostgreSQL<br/>Dimension Tables")]
    H[("PostgreSQL<br/>fact_ethnic_origins")]

    I("DuckDB Engine<br/>In-Process Parquet Copy")
    J[("Parquet File<br/>ZSTD Compressed")]
    K("DuckDB Engine<br/>Analytical SQL Queries")

    A -->|"1. Download Zip"| B
    B -->|"2. Parse CSV Chunks"| C
    C -->|"3. Load Staging Data"| D

    E -->|"4. Fetch Metadata"| F
    F -->|"5. Upsert Dimensions"| G
    D -->|"6. Run SQL Transforms"| H
    G --> H

    D -->|"7. Read Staging Table"| I
    I -->|"8. Write Parquet File"| J
    J -->|"9. Query Parquet Data"| K

    classDef trigger fill:#E0F2FE,stroke:none,color:#0369A1,rx:14px,ry:14px;
    classDef compute fill:#E2F1E6,stroke:none,color:#14532D,rx:14px,ry:14px;
    classDef storage fill:#FEF9C3,stroke:#EAB308,stroke-width:2px,color:#713F12;
    classDef external fill:#FFEDD5,stroke:none,color:#7C2D12,rx:14px,ry:14px;

    class A,E external;
    class B,C,F,I compute;
    class D,G,H,J storage;
    class K trigger;
```

---

## Pipeline Implementation Details

* **In-Memory Downloading (`requests`, `io.BytesIO`):** Downloads large `.zip` files straight into RAM memory so the code runs fast without saving extra temp files to your hard drive.
* **Chunked Ingestion (`Pandas`):** Reads the giant CSV file in 50,000-row batches, fixes column names into clean lowercase format (`snake_case`), and loads them into PostgreSQL without crashing memory.
* **Automatic Dimension Setup (`SQLAlchemy`):** Calls StatCan's REST API (`getCubeMetadata`) to fetch categories (like regions, age groups, and job types), inserting them into 6 dimension tables while ignoring duplicates (`ON CONFLICT DO UPDATE`).
* **Database Transformations (`SQL`):** Uses SQL queries inside PostgreSQL to link raw text rows to their respective dimension IDs, creating a clean central fact table.
* **Parquet Export & Querying (`DuckDB`):** Uses a standalone script (`duckdb_test.py`) with DuckDB to convert raw tables into small, compressed Parquet files (`.parquet`), allowing instant SQL queries over local files just like Amazon Athena does in S3.

---

## Data Model (Snowflake Schema)

The database organizes data into a Snowflake Schema, breaking down complex categories (like sub-regions or job sub-types) into smaller, neat sub-tables before connecting them to the main numbers table.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#F8FAFC' }}}%%
flowchart TD
    subgraph Dimensions ["Normalized Dimension Tables"]
        GEO_PARENT["dim_geo_levels"]
        GEO["dim_geography"]
        
        LAB_PARENT["dim_labour_categories"]
        LAB["dim_labour_force_characteristics"]
        
        GEN["dim_gender"]
        AGE["dim_age_group"]
        STAT["dim_statistics"]
        TYPE["dim_data_type"]
    end

    FACT[("fact_ethnic_origins<br/>(Central Fact Table)")]

    GEO_PARENT -->|"Parent Region ID"| GEO
    LAB_PARENT -->|"Parent Category ID"| LAB

    GEO -->|"geo_id"| FACT
    LAB -->|"labour_id"| FACT
    GEN -->|"gender_id"| FACT
    AGE -->|"age_id"| FACT
    STAT -->|"stat_id"| FACT
    TYPE -->|"type_id"| FACT

    classDef fact fill:#E2F1E6,stroke:#166534,stroke-width:2px,color:#14532D,rx:12px,ry:12px;
    classDef dim fill:#E0F2FE,stroke:#0284C7,stroke-width:1px,color:#0369A1,rx:10px,ry:10px;
    classDef subdim fill:#F3E8FF,stroke:#A855F7,stroke-width:1px,color:#6B21A8,rx:10px,ry:10px;

    class FACT fact;
    class GEO,LAB,GEN,AGE,STAT,TYPE dim;
    class GEO_PARENT,LAB_PARENT subdim;

    style Dimensions fill:#F8FAFC,stroke:#CBD5E1,rx:16px,ry:16px,color:#475569;
```

### Table Structure

* **`staging_ethnic_origins`:** The raw landing table where imported CSV text first arrives.
* **`fact_ethnic_origins`:** The main table holding all numerical counts, rates, and values, linked to dimension IDs.
* **`dim_geography` & `dim_geo_levels`:** Maps locations from country down to provinces and sub-regions.
* **`dim_labour_force_characteristics` & `dim_labour_categories`:** Maps employment categories and job classifications.
* **`dim_gender`:** Gender and sex breakdowns.
* **`dim_age_group`:** Age bracket ranges.
* **`dim_statistics`:** Tells you what type of number it is (count, percentage, rate).
* **`dim_data_type`:** Quality flags and unit descriptors.
