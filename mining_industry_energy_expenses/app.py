import csv
import duckdb

metadata_path = "mining_industry_energy_expenses/16100030_CubeMetaData.csv"
dimension_output_path = (
    "mining_industry_energy_expenses/bronze_layer/dimension_tables.csv"
)


def handler():
    # ==========================================
    # STEP 1: Extract Dimension Table (csv) into Bronze layer from Metadata
    # ==========================================
    extracted_members = []
    inside_member_section = False

    # Generate dimension tables csv
    with open(
        metadata_path,
        "r",
        encoding="utf-8-sig",
    ) as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0] == "" and row[1] == "":
                if inside_member_section is True:
                    break
                else:
                    continue
            # locate demension tables data header
            if (
                row[0] == "Dimension ID"
                and row[1] == "Member Name"
                and row[2] == "Classification Code"
                and row[3] == "Member ID"
                and row[4] == "Parent Member ID"
            ):
                inside_member_section = True

            if inside_member_section:
                extracted_members.append(row[:5])

    # print(extracted_members)

    # Save a csv dimension table under bronze layer
    with open(
        dimension_output_path,
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "dimension_id",
                "member_name",
                "classification_code",
                "member_id",
                "parent_member_id",
            ]
        )
        writer.writerows(extracted_members[1:])

    # ==========================================
    # STEP 2: Flatten & Export to Parquet via DuckDB
    # ==========================================
    # Initialize an in-memory DuckDB connection
    con = duckdb.connect()
    duckdb_transform_query = """
    copy (
        with split_fact as (
            select
                *,
                cast(str_split(coordinate, '.')[1] as int) as geo_member_id,
                cast(str_split(coordinate, '.')[2] as int) as naics_member_id,
                cast(str_split(coordinate, '.')[3] as int) as energy_member_id
            from read_csv_auto('mining_industry_energy_expenses/bronze_layer/16100030.csv')
        )
        SELECT
            cast(f.ref_date as int) as ref_date,
            cast(f.dguid as varchar) as dguid,
            cast(f.uom as varchar) as uom,
            cast(f.vector as varchar) as vector,
            cast(f.coordinate as varchar) as coordinate,
            cast(d1.member_name as varchar) as geography,
            cast(d2.member_name as varchar) as naics_industry,
            cast(d3.member_name as varchar) as energy_type,
            try_cast(f.value as double) as value
        from split_fact f
        left join read_csv_auto('mining_industry_energy_expenses/bronze_layer/dimension_tables.csv') d1
            on cast(d1.dimension_id as int) = 1 and f.geo_member_id = cast(d1.member_id as int)
        left join read_csv_auto('mining_industry_energy_expenses/bronze_layer/dimension_tables.csv') d2
            on cast(d2.dimension_id as int) = 2 and f.naics_member_id = cast(d2.member_id as int)
        LEFT JOIN read_csv_auto('mining_industry_energy_expenses/bronze_layer/dimension_tables.csv') d3 
            ON cast(d3.dimension_id AS int) = 3 and f.energy_member_id = cast(d3.member_id as int)
    ) to 'mining_industry_energy_expenses/silver_layer/cleaned.parquet' (format 'parquet');       
    """
    con.execute(duckdb_transform_query)


if __name__ == "__main__":
    handler()
