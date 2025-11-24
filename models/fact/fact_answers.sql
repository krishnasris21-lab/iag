{{ config(materialized='table') }}

select
    post_id        AS answer_id,
    parent_post_id as question_id,
    owner_user_id  AS answerer_user_id,
    created_at     AS answer_created_at,
    score
from {{ ref('stg_posts') }}
where post_type_id = 2
