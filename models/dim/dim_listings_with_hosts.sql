WITH l AS (
  SELECT *
    FROM {{ ref('dim_listings_cleansed') }}
),
h AS (
  SELECT *
    FROM {{ ref('dim_hosts_cleansed') }}
)
SELECT listing_id,
       listing_name,
       room_type,
       minimum_nights,
       price,
       l.host_id,
       host_name,
       is_superhost                         AS host_is_superhost,
       l.created_at,
       GREATEST(l.updated_at, h.updated_at) AS updated_at
  FROM l
  LEFT JOIN h
 USING (host_id)