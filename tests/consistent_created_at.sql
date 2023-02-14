SELECT *
  FROM {{ ref('dim_listings_cleansed') }} l
  JOIN {{ ref('fact_reviews') }} r
 USING (listing_id)
 WHERE l.created_at >= r.review_date