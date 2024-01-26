{{ config(materialized='table') }}


WITH flattened_markets AS (
    SELECT
        jsonb_array_elements_text(CAST(file_contents AS JSONB)->'markets') AS market_data
    FROM
        {{ source('predictitraw','predictraw') }}
)

SELECT
    enpt.id AS "marketId",
    enpt.name,
    enpt."shortName",
    enpt.image,
    enpt.url,
    enpt."timeStamp",
    enpt.status
FROM flattened_markets,
LATERAL jsonb_to_record(flattened_markets.market_data::jsonb) AS enpt(id integer, name text, "shortName" text, image text, url text, "timeStamp" timestamp, status text)
