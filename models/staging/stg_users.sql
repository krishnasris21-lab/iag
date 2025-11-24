{{ config(materialized='table') }}

select
    Id           as user_id,
    DisplayName  as display_name,
    Reputation,
    CreationDate as account_created_at,
    UpVotes      as up_votes,
    DownVotes    as down_votes,
    Views        as profile_views
from {{ source('iag', 'users') }}
