# IAG StackOverflow dbt + DuckDB Assessment

This repository contains my solution to the IAG SQL & data modelling assessment based on the  dataset (May 2023 snapshot). The project is implemented using **dbt Core** with a **DuckDB** backend.

## Dataset

The source data is the data dump (May 2023) stored as parquet files in S3, as described in the assessment brief:

- `comments.parquet`
- `posts.parquet`
- `votes.parquet`
- `users.parquet`
- `badges.parquet`
- `post_links.parquet`
- `tags.parquet`

All sources are configured under the `iag` source in `models/staging/sources.yml`.

## Tech stack

- **dbt Core** (DuckDB adapter)
- **DuckDB** as the analytical engine
- **Parquet on S3** as the raw data layer
- **Dimensional modelling (star schema)** for questions, answers, comments and users

## Data model overview

The project follows a layered approach:

1. **Staging (`models/staging`)**
   - `stg__posts`
   - `stg_users`
   - `stg_comments`
   - etc.
   - These models:
     - Apply consistent naming
     - Subset to relevant columns
     - Provide a clean base for facts/dims

2. **Dimensions (`models/dim`)**
   - `dim_users`: one row per user
   - `dim_date`: one row per calendar date

3. **Facts (`models/fact`)**
   - `fact_questions`: one row per question
   - `fact_answers`: one row per answer
   - `fact_comments`: one row per comment

4. **Product (`models/marts`)**
   - `users_by_views`
   - `time_to_accepted_bands`
   - `with_at_replies`

These marts correspond directly to the three assessment questions.

### Star schema (high level)

- Fact tables:
  - Questions (`fact=_questions`)
  - Answers (`fact=_answers`)
  - Comments (`fact_comments`)
- Dimension tables:
  - Users (`dim_users`)
  - Date (`dim_date`)

Questions and answers are both stored in the underlying `posts` source and separated by `PostTypeId`. The fact tables then reference the user and date dimensions.

## Assessment questions and how they are solved

### Q1: Top 10 users based on having the most views on questions they post

Implemented in:

- `models/marts/users_by_views.sql`

Logic:

- Start from `fact_questions`
- Aggregate `view_count` by `asker_user_id`
- Join `dim_users` for display_name
- Order by total views and take the top 10

### Q2: Time series of question counts by time-to-accepted-answer band

Implemented in:

- `models/marts/time_to_accepted_bands.sql`

Logic:

- Join `fact_questions` (questions) with `fact_answers` (accepted answers)
- Compute the time difference between question creation and accepted answer creation
- Bucket into bands:
  - `<1 min`, `1–5 mins`, `5 mins–1 hour`, `1–3 hours`, `3 hours–1 day`, `>1 day`, `no accepted`
- Aggregate counts per month across bands

### Q3: Questions with @-replies by the asker

Implemented in:

- `models/marts/with_at_replies.sql`

Logic:

- Build a set of posts related to each question:
  - The question itself
  - All of its answers
- Join to `fact_comments`
- Filter to comments where:
  - `comments.user_id = asker_user_id`
  - `comments.text` contains `'@'`
- Count the distinct number of questions meeting this condition

## Tests and data validation

- Source tests on `iag` (implicit via dbt)
- tests specified in:
  - `_models.yml`
- Key tests:
  - `not_null` and `unique` on natural keys like `question_id`, `answer_id`, `comment_id`, `user_id`
