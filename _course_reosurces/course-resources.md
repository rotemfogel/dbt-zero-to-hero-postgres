# Introduction and Environment Setup

## Start the PostgreSQL database
start the docker
```bash
docker-compose up -d 
```

## PostgreSQL user creation
Connect to postgres database
```bash
psql -h 127.0.0.1 -U postgres
```
Paste following SQL Statements
```sql
-- Create the default database
CREATE DATABASE airbnb;

-- Create the `dbt` user and assign to role
CREATE USER dbt WITH ENCRYPTED PASSWORD 'dbtPassword123';

-- Grant permissions to dbt on database airbnb
GRANT ALL ON DATABASE airbnb to dbt;
```

## PostgreSQL data import
Download the files from S3
```bash
wget  https://dbtlearn.s3.amazonaws.com/hosts.csv
wget  https://dbtlearn.s3.amazonaws.com/reviews.csv
wget  https://dbtlearn.s3.amazonaws.com/listings.csv
```
Paste following SQL Statement
```sql
-- Create our three tables and import the data from S3
CREATE TABLE raw.raw_listings
  (id integer not null primary key,
   listing_url text,
   name text,
   room_type text,
   minimum_nights integer,
   host_id integer,
   price varchar(10),
   created_at timestamp,
   updated_at timestamp);
  
\COPY raw.raw_listings (id, listing_url, name, room_type, minimum_nights, host_id, price, created_at, updated_at) FROM 'listings.csv' WITH DELIMITER ',' CSV HEADER; 

CREATE TABLE raw.raw_reviews
  (listing_id integer not null,
   date timestamp not null,
   reviewer_name varchar(64) not null,
   comments text,
   sentiment varchar(10));
CREATE INDEX raw_reviews_listing_id_ix ON raw.raw_reviews(listing_id);
  
\COPY raw.raw_reviews (listing_id, date, reviewer_name, comments, sentiment) FROM 'reviews.csv' WITH DELIMITER ',' QUOTE '"' CSV HEADER; 

CREATE TABLE raw.raw_hosts
  (id integer not null primary key,
   name varchar(36),
   is_superhost varchar(2),
   created_at timestamp,
   updated_at timestamp);
  
\COPY raw.raw_hosts (id, name, is_superhost, created_at, updated_at)  FROM 'hosts.csv' WITH DELIMITER ',' QUOTE '"' CSV HEADER;
```

# Python and Virtualenv setup, and dbt installation - Windows

## Python
This is the Python installer you want to use: 

https://www.python.org/ftp/python/3.10.7/python-3.10.7-amd64.exe 

Please make sure that you work with Python 3.11 as newer versions of python might not be compatible with some of the dbt packages.

## Virtualenv setup
Here are the commands we executed in this lesson:
```
cd Desktop
mkdir course
cd course

virtualenv venv
venv\Scripts\activate
```

## dbt installation

__Make sure that you are working in the _Desktop/course_ folder and that you have virtualenv activated before installing dbt.__

Here are the commands we executed in this lesson:
```
pip install dbt-postgres==1.2.0
dbt
```

# Virtualenv setup and dbt installation - Mac

## iTerm2
We suggest you to use _iTerm2_ instead of the built-in Terminal application.

https://iterm2.com/

## Homebrew
Homebrew is a widely popular application manager for the Mac. This is what we use in the class for installing a virtualenv.

https://brew.sh/

## dbt installation

Here are the commands we execute in this lesson:

```sh
create course
cd course
virtualenv venv
. venv/bin/activate
rehash
pip install dbt-PostgreSQL
which dbt
```

# Models
## Code used in the lesson

### SRC Listings 
`models/staging/staging_listings.sql`:

```sql
WITH raw_listings AS (
  SELECT *
    FROM AIRBNB.RAW.RAW_LISTINGS
)
SELECT id              AS listing_id,
       name            AS listing_name,
       listing_url,
       room_type,
       minimum_nights,
       host_id,
       price           AS price_str,
       created_at,
       updated_at
  FROM raw_listings
```

### SRC Reviews
`models/staging/staging_reviews.sql`:

```sql
WITH raw_reviews AS (
  SELECT *
    FROM AIRBNB.RAW.RAW_REVIEWS
)
SELECT listing_id,
       date            AS review_date,
       reviewer_name,
       comments        AS review_text,
       sentiment       AS review_sentiment
  FROM raw_reviews
```


## Exercise

Create a model which builds on top of our `raw_hosts` table. 

1) Call the model `models/staging/staging_hosts.sql`
2) Use a CTE (common table expression) to define an alias called `raw_hosts`. This CTE select every column from the raw hosts table `AIRBNB.RAW.RAW_HOSTS`
3) In your final `SELECT`, select every column and record from `raw_hosts` and rename the following columns:
   * `id` to `host_id`
   * `name` to `host_name` 

### Solution

```sql
WITH raw_hosts AS (
  SELECT *
    FROM AIRBNB.RAW.RAW_HOSTS
)
SELECT id           AS host_id,
       name         AS host_name,
       is_superhost,
       created_at,
       updated_at
  FROM raw_hosts
```

# Models
## Code used in the lesson

### DIM Listings 
`models/dim/dim_listings_cleansed.sql`:

```sql
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
       REPLACE(price_str, '$', '')::NUMERIC(10, 2) AS price,
       created_at,
       updated_at
  FROM staging_listings
```

### DIM hosts
`models/dim/dim_hosts_cleansed.sql`:

```sql
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
```

## Exercise

Create a new model in the `models/dim/` folder called `dim_hosts_cleansed.sql`.
 * Use a CTE to reference the `staging_hosts` model
 * SELECT every column and every record, and add a cleansing step to host_name:
   * If host_name is not null, keep the original value 
   * If host_name is null, replace it with the value ‘Anonymous’
   * Use the NVL(column_name, default_null_value) function 
Execute `dbt run` and verify that your model has been created 


### Solution

```sql
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
```

## Incremental Models
The `fact/fact_reviews.sql` model:
```sql
{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

WITH staging_reviews AS (
  SELECT *
    FROM {{ ref('staging_reviews') }}
)
SELECT {{ dbt_utils.generate_surrogate_key(
         ['listing_id', 'review_date', 'reviewer_name', 'review_text']
       ) }} AS review_id,
       *
  FROM staging_reviews
 WHERE review_text IS NOT NULL
{% if is_incremental() %}
   AND review_date > (SELECT MAX(review_date)
                        FROM {{ this }} )
{% endif %}
```

Get every review for listing _3176_:
```sql
SELECT * 
  FROM airbnb.dev.fact_reviews
 WHERE listing_id=3176;
```

Add a new record to the table:
```sql
INSERT INTO raw.raw_reviews
VALUES (3176, CURRENT_TIMESTAMP, 'Rotem', 'excellent stay!', 'positive');
```

Making a full-refresh:
```
dbt run --full-refresh
```
## DIM listings with hosts
The contents of `dim/dim_listings_with_hosts.sql`:
```sql
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
```

## Dropping the views after ephemeral materialization
```sql
DROP VIEW AIRBNB.DEV.STAGING_HOSTS;
DROP VIEW AIRBNB.DEV.STAGING_LISTINGS;
DROP VIEW AIRBNB.DEV.STAGING_REVIEWS;
```

# Sources and Seeds

## Full Moon Dates CSV
Download the CSV from the lesson's _Resources_ section, or download it from the following S3 location:
https://dbtlearn.s3.us-east-2.amazonaws.com/seed_full_moon_dates.csv

Then place it to the `seeds` folder

If you download from S3 on a Mac/Linux, can you import the csv straight to your seed folder by executing this command:
```sh
curl https://dbtlearn.s3.us-east-2.amazonaws.com/seed_full_moon_dates.csv -o seeds/seed_full_moon_dates.csv
```

## Contents of models/sources.yml
```yaml
---
version: 2

sources:
  - name: airbnb
    schema: raw
    tables:
    - name: listings
      identifier: raw_listings

    - name: hosts
      identifier: raw_hosts

    - name: reviews
      identifier: raw_reviews
      loaded_at_field: date
      freshness:
        error_after:
          count: 24
          period: hour
        warn_after:
          count: 1
          period: hour
```

## Contents of models/mart/mart_full_moon_reviews.sql
```sql
{{
    config(
        materialized = 'table'
    )
}}

WITH fact_reviews AS (
    SELECT * FROM {{ ref('fact_reviews') }}
),
full_moon_dates AS (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
)
SELECT r.*,
       CASE
         WHEN fm.full_moon_date IS NULL
           THEN false
         ELSE true
       END AS is_full_moon
  FROM fact_reviews r
  LEFT JOIN full_moon_dates fm
    ON (DATE(r.review_date) = fm.full_moon_date + INTERVAL '1' DAY)
```

# Snapshots

## Snapshots for listing
The contents of `snapshots/scd_raw_listings.sql`:

```sql
{% snapshot scd_raw_listings %}

{{
    config(
        target_schema='dev',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT *
 FROM {{ source('airbnb', 'listings') }}

{% endsnapshot %}
```

### Updating the table
```sql
UPDATE airbnb.raw.raw_listings 
   SET minimum_nights = 30,
       updated_at     = CURRENT_TIMESTAMP() 
 WHERE id=3176;

SELECT * 
  FROM airbnb.dev.scd_raw_listings 
 WHERE ID=3176;
```

## Snapshots for hosts
The contents of `snapshots/scd_raw_hosts.sql`:
```sql
{% snapshot scd_raw_hosts %}

{{
   config(
       target_schema='dev',
       unique_key='id',
       strategy='timestamp',
       updated_at='updated_at',
       invalidate_hard_deletes=True
   )
}}

SELECT * 
  FROM {{ source('airbnb', 'hosts') }}

{% endsnapshot %}
```

# Tests

## Generic Tests
The contents of `models/schema.yml`:

```sql
version: 2

models:
  - name: dim_listings_cleansed
    columns:

     - name: listing_id
       tests:
         - unique
         - not_null

     - name: host_id
       tests:
         - not_null
         - relationships:
             to: ref('dim_hosts_cleansed')
             field: host_id

     - name: room_type
       tests:
         - accepted_values:
             values: ['Entire home/apt',
                      'Private room',
                      'Shared room',
                      'Hotel room']
```

### Generic test for minimum nights check
The contents of `tests/dim_listings_minumum_nights.sql`:

```sql
SELECT *
  FROM {{ ref('dim_listings_cleansed') }}
 WHERE minimum_nights < 1
 LIMIT 10
```

### Restricting test execution to a model
```sh
dbt test --select dim_listings_cleansed
```

## Exercise

Create a singular test in `tests/consistent_created_at.sql` that checks that there is no review date that is submitted before its listing was created: Make sure that every `review_date` in `FACT_REVIEWS` is more recent than the associated `created_at` in `dim_listings_cleansed`.


### Solution
```sql
SELECT *
  FROM {{ ref('dim_listings_cleansed') }} l
  JOIN {{ ref('fact_reviews') }} r
 USING (listing_id)
 WHERE l.created_at >= r.review_date
```
# Marcos, Custom Tests and Packages 
## Macros

The contents of `macros/no_nulls_in_columns.sql`:
```sql
{% macro no_nulls_in_columns(model) %}
SELECT *
  FROM {{ model }}
 WHERE {% for col in adapter.get_columns_in_relation(model) -%}
       {{ col.column }} IS NULL OR
       {% endfor -%}
       FALSE
{% endmacro %}
```

The contents of `tests/no_nulls_in_dim_listings.sql`
```sql
{{ no_nulls_in_columns(ref('dim_listings_cleansed')) }}
```

## Custom Generic Tests
The contents of `macros/positive_value.sql`
```sql
{% test positive_value(model, column_name) %}
SELECT *
  FROM {{ model }}
 WHERE {{ column_name}} < 1
{% endtest %}
```

## Packages
The contents of `packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.0.0
```

The contents of ```models/fact_reviews.sql```:
```
{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}

WITH staging_reviews AS (
  SELECT *
    FROM {{ ref('staging_reviews') }}
)
SELECT {{ dbt_utils.generate_surrogate_key(
         ['listing_id', 'review_date', 'reviewer_name', 'review_text']
       ) }} AS review_id,
       *
  FROM staging_reviews
 WHERE review_text IS NOT NULL
{% if is_incremental() %}
   AND review_date > (SELECT MAX(review_date)
                        FROM {{ this }} )
{% endif %}
```

## Documentation

The `models/schema.yml` after adding the documentation:
```yaml
version: 2

models:
  - name: dim_listings_cleansed
    description: Cleansed table which contains Airbnb listings.
    columns:
      - name: listing_id
        description: Primary key for the listing
        tests:
          - unique
          - not_null
      - name: host_id
        description: The hosts's id. References the host table.
        tests:
          - not_null
          - relationships:
              to: ref('dim_hosts_cleansed')
              field: host_id
      - name: room_type
        description: Type of the apartment / room
        tests:
          - accepted_values:
              values: [ 'Entire home/apt',
                        'Private room',
                        'Shared room',
                        'Hotel room' ]
      - name: minimum_nights
        description: '{{ doc("dim_listing_cleansed__minimum_nights") }}'
        tests:
          - positive_value

  - name: dim_hosts_cleansed
    columns:
      - name: host_id
        tests:
          - not_null
          - unique

      - name: host_name
        tests:
          - not_null

      - name: is_superhost
        tests:
          - accepted_values:
              values: [ 't', 'f' ]

  - name: fact_reviews
    columns:
      - name: listing_id
        tests:
          - relationships:
              to: ref('dim_listings_cleansed')
              field: listing_id

      - name: reviewer_name
        tests:
          - not_null

      - name: review_sentiment
        tests:
          - accepted_values:
              values: [ 'positive', 'neutral', 'negative' ]
```
The contents of `models/docs.md`:
```txt
{% docs dim_listing_cleansed__minimum_nights %}
Minimum number of nights required to rent this property. 

Keep in mind that old listings might have `minimum_nights` set 
to 0 in the source tables. Our cleansing algorithm updates this to `1`.

{% enddocs %}
```

The contents of `models/overview.md`:
```md
{% docs __overview__ %}
# Airbnb pipeline

Hey, welcome to our Airbnb pipeline documentation!

Here is the schema of our input data:
![input schema](https://dbtlearn.s3.us-east-2.amazonaws.com/input_schema.png)

{% enddocs %}
```

# Analyses, Hooks and Exposures

## Create the REPORTER role and PRESET user in PostgreSQL
Connect to postgres database
```bash
psql -h 127.0.0.1 -U postgres -d airbnb
```
```sql
CREATE USER preset WITH ENCRYPTED PASSWORD 'presetPassword123';
CREATE ROLE reporter;
GRANT reporter TO preset;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO reporter;
GRANT SELECT ON ALL TABLES IN SCHEMA dev TO reporter;
```

## Analyses
The contents of `analyses/full_moon_no_sleep.sql`:
```sql
WITH mart_fullmoon_reviews AS (
  SELECT *
    FROM {{ ref('mart_fullmoon_reviews') }}
)
SELECT is_full_moon,
       review_sentiment,
       COUNT(*) AS reviews
  FROM mart_fullmoon_reviews
 GROUP BY is_full_moon,
          review_sentiment
 ORDER BY is_full_moon,
          review_sentiment
```

## Exposures
The contents of `models/dashboard.yml`:
```yaml
version: 2

exposures:
  - name: Executive Dashboard
    type: dashboard
    maturity: low
    url: https://7e942fbd.us2a.app.preset.io:443/r/2
    description: Executive Dashboard about Airbnb listings and hosts
      

    depends_on:
      - ref('dim_listings_w_hosts')
      - ref('mart_fullmoon_reviews')

    owner:
      name: Rotem Fogel
      email: rotemfo@gmail.com
```

## Post-hook
Add this to your `dbt_project.yml`:

```
+post-hook:
  - "GRANT SELECT ON {{ this }} TO ROLE REPORTER"
```

# Debugging Tests and Testing with dbt-expectations

* The original Great Expectations project on GitHub: https://github.com/great-expectations/great_expectations
* dbt-expectations: https://github.com/calogica/dbt-expectations 

For the final code in _packages.yml_, _models/schema.yml_ and _models/sources.yml_, please refer to the course's Github repo:
https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero

## Testing a single model

```
dbt test --select dim_listings_w_hosts
```

Testing individual sources:

```
dbt test --select source:airbnb.listings
```

## Debugging dbt

```
dbt --debug test --select dim_listings_w_hosts
```

Keep in mind that in the lecture we didn't use the _--debug_ flag after all as taking a look at the compiled sql file is the better way of debugging tests.


