{{ config(materialized='view') }}

with posts_for_q as (
  select
    q.question_id,
    q.asker_user_id,
    q.question_id as post_id
  from {{ ref('fact_questions') }} q

  union all

  select
    a.question_id,
    q.asker_user_id,
    a.answer_id as post_id
  from {{ ref('fact_answers') }} a
  join {{ ref('fact_questions') }} q
    on q.question_id = a.question_id
),
asker_comments as (
  select distinct
    p.question_id
  from posts_for_q p
  join {{ ref('fact_comments') }} c
    on c.post_id = p.post_id
   and c.user_id = p.asker_user_id
   and c.text ilike '%@%'
)
select
  count(1) as questions_with_at_replies
from asker_comments
