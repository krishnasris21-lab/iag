{{ config(materialized='table') }}

SELECT *
FROM read_parquet('s3://us-prd-motherduck-open-datasets/stackoverflow/parquet/2023-05/posts.parquet')
