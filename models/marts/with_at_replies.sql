{{ config(materialized='view') }}
 
with questions as (
  select
    Id           as question_id,
    OwnerUserId  as asker_user_id
  from {{ ref('stg_posts') }}
  where PostTypeId = 1
),
answers as (
  select
    Id         as answer_id,
    ParentId   as question_id
  from {{ ref('stg_posts') }}
  where PostTypeId = 2
),
posts_for_q as (
  select q.question_id, q.asker_user_id, q.question_id as post_id
  from questions q
  union all
  select a.question_id, q.asker_user_id, a.answer_id as post_id
  from answers a
  join questions q on q.question_id = a.question_id
),
asker_comments as (
  select distinct p.question_id
  from posts_for_q p
  join {{ ref('stg_comments') }} c
    on c.PostId = p.post_id
   and c.UserId = p.asker_user_id
   and c.Text ilike '%@%'  
)
select
  count(*) as questions_with_at_replies
from asker_comments