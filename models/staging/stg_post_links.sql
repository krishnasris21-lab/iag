{{ config(materialized='table') }}

SELECT *
FROM from {{ source('iag', 'post_links') }}
