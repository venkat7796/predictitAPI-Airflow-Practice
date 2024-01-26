-- models/contracts.sql

-- Assuming your source table is named "source_table" and it contains a JSONB column named "json_data"
-- Adjust the table and column names according to your actual source structure

{{ config(materialized='table') }}



WITH flattened_markets AS (
    SELECT
        jsonb_array_elements_text(CAST(file_contents AS JSONB)->'markets') AS market_data
    FROM
        {{ source('predictitraw','predictraw') }}
),

flattened_contracts AS (
SELECT
    enpt.id AS "marketId",
    enpt.name,
    enpt."shortName",
    enpt.image,
    enpt.url,
    enpt."timeStamp",
    enpt.status,
	contracts
FROM flattened_markets,
LATERAL jsonb_to_record(flattened_markets.market_data::jsonb) AS enpt(id integer, name text, "shortName" text, image text, url text, "timeStamp" timestamp, status text,contracts jsonb)
	)
	
SELECT "marketId",
cont1.id AS "contractId",
    cont1."dateEnd",
    cont1.image,
    cont1.name,
    cont1."shortName",
    cont1.status,
    cont1."lastTradePrice",
    cont1."bestBuyYesCost",
    cont1."bestBuyNoCost",
    cont1."bestSellYesCost",
    cont1."bestSellNoCost",
    cont1."lastClosePrice",
    cont1."displayOrder"
FROM flattened_contracts,
LATERAL jsonb_to_recordset(contracts) AS cont1(id integer,"dateEnd" text,image text,name text, "shortName" text,status text,"lastTradePrice" float,"bestBuyYesCost" float,"bestBuyNoCost" float,"bestSellYesCost" float,"bestSellNoCost" float,"lastClosePrice" float,"displayOrder" integer)

