{{ config(materialized='table') }}

select
    comment_id,
    post_id,
    user_id,
    text,
    comment_created_at,
    score
from {{ ref('stg_comments') }}
