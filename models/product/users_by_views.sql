{{ config(materialized='view') }}

with question_views as (
    select
        asker_user_id,
        sum(view_count) as total_question_views
    from {{ ref('fact_questions') }}
    group by 1
),
joined as (
    select
        q.asker_user_id as user_id,
        coalesce(u.display_name, cast(q.asker_user_id as varchar)) as user_display_name,
        q.total_question_views
    from question_views q
    left join {{ ref('dim_users') }} u
      on u.user_id = q.asker_user_id
)

select *
from joined
order by total_question_views desc
limit 10
