{{ config(materialized='view') }}
 
with questions as (
  select
    Id              as question_id,
    OwnerUserId     as asker_user_id,
    ViewCount       as view_count
  from {{ ref('stg_posts') }}
  where PostTypeId = 1  -- question
    and OwnerUserId is not null
),
users as (
  select Id as user_id, DisplayName
  from {{ ref('stg_users') }}
)
 
select
  q.asker_user_id as user_id,
  coalesce(u.DisplayName, cast(q.asker_user_id as varchar)) as user_display_name,
  sum(q.view_count) as total_question_views
from questions q
left join users u on u.user_id = q.asker_user_id
group by 1,2
order by total_question_views desc
limit 10