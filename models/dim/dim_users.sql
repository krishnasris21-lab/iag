{{ config(materialized='table') }}

select
    user_id,
    display_name,
    reputation,
    account_created_at,
    up_votes,
    down_votes,
    profile_views
from {{ ref('stg_users') }}
