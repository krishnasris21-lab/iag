{{ config(materialized='view') }}
 
with q as (
  select
    Id               as question_id,
    CreationDate     as question_created_at,
    AcceptedAnswerId
  from {{ ref('stg_posts') }}
  where PostTypeId = 1
),
a as (
  select
    Id               as answer_id,
    CreationDate     as answer_created_at
  from {{ ref('stg_posts') }}
  where PostTypeId = 2
),
qa as (
  select
    date_trunc('month', q.question_created_at)::date as month,
    q.question_created_at,
    a.answer_created_at as accepted_created_at
  from q
  left join a on a.answer_id = q.AcceptedAnswerId
),
banded as (
  select
    month,
    case
      when accepted_created_at is null                               then 'no accepted'
      when (accepted_created_at - question_created_at) < interval '1 minute'  then '<1 min'
      when (accepted_created_at - question_created_at) < interval '5 minutes' then '1-5 mins'
      when (accepted_created_at - question_created_at) < interval '1 hour'    then '5 mins-1 hour'
      when (accepted_created_at - question_created_at) < interval '3 hours'   then '1-3 hours'
      when (accepted_created_at - question_created_at) < interval '1 day'     then '3 hours-1 day'
      else '>1 day'
    end as band
  from qa
)
select
  month,
  sum(case when band = '<1 min'         then 1 else 0 end) as lt_1_min,
  sum(case when band = '1-5 mins'       then 1 else 0 end) as _1_5_mins,
  sum(case when band = '5 mins-1 hour'  then 1 else 0 end) as _5m_1h,
  sum(case when band = '1-3 hours'      then 1 else 0 end) as _1_3h,
  sum(case when band = '3 hours-1 day'  then 1 else 0 end) as _3h_1d,
  sum(case when band = '>1 day'         then 1 else 0 end) as gt_1d,
  sum(case when band = 'no accepted'    then 1 else 0 end) as no_accepted
from banded
group by 1
order by 1