{{
    config(
        materialized = 'view'
    )
}}

WITH staging_hosts AS (
  SELECT *
    FROM {{ ref('staging_hosts') }}
)
SELECT host_id,
       COALESCE(host_name, 'Anonymous') AS host_name,
       is_superhost,
       created_at,
       updated_at
  FROM staging_hosts