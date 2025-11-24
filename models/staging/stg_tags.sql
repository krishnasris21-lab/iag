{{ config(materialized='table') }}

SELECT *
FROM from {{ source('iag', 'tags') }}
