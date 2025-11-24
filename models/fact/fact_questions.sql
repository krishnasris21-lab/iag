{{ config(materialized='table') }}

select
    post_id              as question_id,
    owner_user_id        as asker_user_id,
    created_at           as question_created_at,
    view_count,
    score,
    accepted_answer_id
from {{ ref('stg_posts') }}
where post_type_id = 1
  and owner_user_id IS NOT NULL
