{{
    config(
        materialized = 'view'
    )
}}

WITH staging_listings AS (
  SELECT *
    FROM {{ ref('staging_listings') }}
)
SELECT listing_id,
       listing_name,
       room_type,
       CASE
         WHEN minimum_nights = 0
           THEN 1
         ELSE minimum_nights
       END                                      AS minimum_nights,
       host_id,
       REPLACE(CAST(price_str AS VARCHAR), '$', '')::NUMERIC(10, 2) AS price,
       created_at,
       updated_at
  FROM staging_listings