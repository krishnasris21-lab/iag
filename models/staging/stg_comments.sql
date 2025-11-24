{{ config(materialized='table') }}

select
    id AS comment_id,
    postid AS post_id,
    userid AS user_id,
    text,
    creationdate     AS comment_created_at,
    score
from {{ source('iag', 'comments') }}
