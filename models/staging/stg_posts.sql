{{ config(materialized='table') }}

select
    Id                as post_id,
    PostTypeId        as post_type_id,
    ParentId          as parent_post_id,
    AcceptedAnswerId  as accepted_answer_id,
    OwnerUserId       as owner_user_id,
    CreationDate      as created_at,
    Score             as score,
    ViewCount         as view_count,
    LastActivityDate  as last_activity_at,
    FavoriteCount     as favorite_count,
    Title,
    Tags,
    AnswerCount,
    CommentCount
from {{ source('iag', 'posts') }}
