# StatCan Data Lakehouse & ELT Pipelines

A data engineering project exploring two pipeline patterns for processing Statistics Canada census datasets: API streaming with Pandas into PostgreSQL, and direct CSV parsing with DuckDB into partitioned Bronze/Silver Parquet layers.

---

## Technical Stack

* **Languages:** Python 3.13, SQL
* **Relational Database:** PostgreSQL 15+ (Staging Warehouse & Snowflake Schema)
* **Analytical Engine:** DuckDB (In-memory CSV parsing, Parquet export, and SQL-on-Parquet queries)
* **Data Libraries & Tools:** Pandas, SQLAlchemy, `requests`, Jupyter Notebook
* **Environment:** Docker, WSL 2 (Ubuntu Linux)

---

## Project Overview

This project explores two pipeline workflows using national demographic and economic census datasets from Statistics Canada:

* **Pipeline 1 (StatCan ID 14100287 - API Ingestion & Pandas):** Streams zip archives from the StatCan REST API, parses records in 50,000-row Pandas batches into a PostgreSQL Snowflake Schema, and exports Parquet files for DuckDB querying.
* **Pipeline 2 (StatCan ID 16100030 - Direct CSV & DuckDB Lakehouse):** Uses DuckDB's in-memory engine to parse raw CSV files directly in RAM, writing cleaned tables directly into Bronze and Silver Parquet directory layers for partitioned lakehouse querying.

Both pipelines are built to mirror an **AWS Cloud Data Lakehouse** setup (Amazon Redshift, S3, and Amazon Athena) by managing structured relational schemas in PostgreSQL and running serverless SQL queries on compressed Parquet files using DuckDB and Jupyter Notebooks.

---

## Pipeline Architecture

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground': '#F8FAFC' }}}%%
flowchart TD
    subgraph Ingestion ["1. Data Ingestion & Parsing"]
        A1("StatCan REST API ID 14100287<br/>Zip Endpoint") -->|"Pipeline 1: BytesIO Stream"| B1("Pandas Parser<br/>50,000 Row Batches")
        A2("StatCan CSV ID 16100030<br/>Raw Data File") -->|"Pipeline 2: Direct RAM Read"| B2("DuckDB In-Memory Engine")
    end

    subgraph Transformation ["2. Relational Warehouse & Storage Layers"]
        B1 -->|"Load Landing Data"| C1[("PostgreSQL<br/>staging_ethnic_origins")]
        D1("StatCan Metadata API<br/>getCubeMetadata") -->|"SQLAlchemy Upsert"| E1[("PostgreSQL<br/>Dimension Tables")]
        C1 & E1 -->|"In-Database SQL Transforms"| F1[("PostgreSQL<br/>fact_ethnic_origins")]
        
        B2 -->|"Clean & Standardize"| C2[("Bronze Layer<br/>Parquet Storage")]
        C2 -->|"Aggregate & Partition"| D2[("Silver Layer<br/>Partitioned Parquet")]
    end

    subgraph Analytics ["3. Lakehouse Analytics"]
        F1 -->|"Export Parquet"| G("DuckDB Engine / Jupyter Notebooks<br/>Serverless SQL Queries")
        D2 -->|"Read Parquet Layers"| G
    end

    classDef trigger fill:#E0F2FE,stroke:none,color:#0369A1,rx:14px,ry:14px;
    classDef compute fill:#E2F1E6,stroke:none,color:#14532D,rx:14px,ry:14px;
    classDef storage fill:#FEF9C3,stroke:#EAB308,stroke-width:2px,color:#713F12;
    classDef external fill:#FFEDD5,stroke:none,color:#7C2D12,rx:14px,ry:14px;

    class A1,D1,A2 external;
    class B1,B2 compute;
    class C1,E1,F1,C2,D2 storage;
    class G trigger;

    style Ingestion fill:#F8FAFC,stroke:#CBD5E1,rx:16px,ry:16px,color:#334155;
    style Transformation fill:#F8FAFC,stroke:#CBD5E1,rx:16px,ry:16px,color:#334155;
    style Analytics fill:#F8FAFC,stroke:#CBD5E1,rx:16px,ry:16px,color:#334155;
```

---

## Pipeline Implementation Details

* **Pipeline 1 Ingestion (`requests`, `io.BytesIO`, `Pandas`):** Streams Product ID 14100287 zip files directly into RAM memory to avoid writing temp files to disk. Parses the raw CSV in 50,000-row batches, converts column names to `snake_case`, and appends them into PostgreSQL staging.
* **Pipeline 2 Ingestion (`DuckDB`):** Uses DuckDB's native vectorized CSV reader to parse Product ID 16100030 raw files directly in memory, bypassing Pandas chunking and relational staging entirely.
* **Automatic Dimension Setup (`SQLAlchemy`):** Fetches dimension categories from StatCan's REST API (`getCubeMetadata`) and inserts them into 6 dimension tables while ignoring duplicates (`ON CONFLICT DO UPDATE`).
* **Database Transformations (`SQL`):** Uses SQL queries inside PostgreSQL to link raw text rows to dimension IDs, populating a central fact table.
* **Bronze & Silver Lakehouse Storage (`DuckDB`):** Pipeline 2 structures exported data into dedicated Bronze (cleaned Parquet) and Silver (partitioned analytical) folders, mimicking data lakehouse storage layouts like Apache Iceberg on Amazon S3.
* **SQL Querying (`DuckDB`, `Jupyter Notebook`):** Both pipelines support running fast SQL queries over compressed Parquet files using DuckDB inside Jupyter Notebooks, providing a local alternative to Amazon Athena or Redshift Spectrum.

---

## Data Model (Snowflake Schema)

The database organizes dimensional relationships into a Snowflake Schema, breaking down complex categories (such as sub-regions or employment sub-types) into smaller sub-tables before connecting them to the central fact table.

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
* **`fact_ethnic_origins`:** The central fact table holding numerical counts, rates, and values linked to dimension IDs.
* **`dim_geography` & `dim_geo_levels`:** Maps locations from national down to provinces and sub-regions.
* **`dim_labour_force_characteristics` & `dim_labour_categories`:** Maps employment categories and job classifications.
* **`dim_gender`:** Gender and sex demographic classifications.
* **`dim_age_group`:** Age range brackets.
* **`dim_statistics`:** Tells you the measure type (count, percentage, rate).
* **`dim_data_type`:** Quality flags and unit descriptors.
