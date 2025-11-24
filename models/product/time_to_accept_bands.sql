{{ config(materialized='view') }}

with q as (
  select
    question_id,
    question_created_at,
    accepted_answer_id
  from {{ ref('fact_questions') }}
),
a as (
  select
    answer_id,
    answer_created_at
  from {{ ref('fact_answers') }}
),
qa as (
  select
    date_trunc('month', q.question_created_at)::date as month,
    q.question_created_at,
    a.answer_created_at
  from q
  left join a on a.answer_id = q.accepted_answer_id
),
banded as (
  select
    month,
    case
      when answer_created_at is null then 'no accepted'
      when (answer_created_at - question_created_at) < interval '1 minute'  then '<1 min'
      when (answer_created_at - question_created_at) < interval '5 minutes' then '1-5 mins'
      when (answer_created_at - question_created_at) < interval '1 hour'    then '5 mins-1 hour'
      when (answer_created_at - question_created_at) < interval '3 hours'   then '1-3 hours'
      when (answer_created_at - question_created_at) < interval '1 day'     then '3 hours-1 day'
      else '>1 day'
    end as band
  from qa
)
select
  month,
  count_if(band = '<1 min')        as lt_1_min,
  count_if(band = '1-5 mins')      as _1_5_mins,
  count_if(band = '5 mins-1 hour') as _5m_1h,
  count_if(band = '1-3 hours')     as _1_3h,
  count_if(band = '3 hours-1 day') as _3h_1d,
  count_if(band = '>1 day')        as gt_1d,
  count_if(band = 'no accepted')   as no_accepted
from banded
group by 1
order by 1
