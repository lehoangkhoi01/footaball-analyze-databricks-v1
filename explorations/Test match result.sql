USE CATALOG `football-analyze-v1`;
USE SCHEMA `gold`;

WITH home_team_summary AS (
  SELECT
    home_team_id as team_id,
    count_if(
      home_team_id = 50 AND goals_home > goals_away
    ) as count_win
  FROM dim_fixtures
  WHERE home_team_id = 50
  GROUP BY home_team_id
),

away_team_summary AS (
  SELECT
    away_team_id as team_id,
    count_if(
      away_team_id = 50 AND goals_away > goals_home
    ) as count_win
  FROM dim_fixtures
  WHERE away_team_id = 50
  GROUP BY away_team_id
),

fact_team_match_result AS (
  SELECT 
    team_name,
    -- win,
    -- draw,
    -- lose
    count_if(win == true) as win_count,
    count_if(draw == true) as draw_count,
    count_if(lose == true) as lose_count
  FROM fact_team_match_result
  WHERE team_name = 'Manchester City'
  GROUP BY team_name
),

home_fact_match_result AS  (
  SELECT
    home_team_name as team_name,
    count_if(is_home_win == true) as win_count,
    count_if(is_draw == true) as draw_count,
    count_if(is_away_win == true) as lose_count
  FROM fact_match_result
  WHERE home_team_name = 'Manchester City'
  GROUP BY home_team_name
),

away_fact_match_result AS (
  SELECT
    away_team_name as team_name,
    count_if(is_away_win == true) as win_count,
    count_if(is_draw == true) as draw_count,
    count_if(is_home_win == true) as lose_count
  FROM fact_match_result
  WHERE away_team_name = 'Manchester City'
  GROUP BY away_team_name
)

SELECT
  *
FROM fact_team_match_result
