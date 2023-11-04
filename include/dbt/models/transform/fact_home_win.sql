-- fact_home_win.sql

-- Create a fact table for home win percentage
WITH home_win AS (
    SELECT
        ROW_NUMBER() OVER () AS home_win_percentage_id,
        m.tournament_name,
        m.match_date,
        m.match_city,
        m.match_country,
        m.match_neutral,
        COUNT(*) AS total_matches,
        SUM(CASE WHEN r.home_score > r.away_score THEN 1 ELSE 0 END) AS home_wins
    FROM {{ ref('fact_match_results') }} r
    JOIN {{ ref('dim_matches') }} m ON r.match_id = m.match_id
    GROUP BY m.tournament_name, m.match_date, m.match_city, m.match_country, m.match_neutral
)
SELECT
    tournament_name,
    match_date,
    match_city,
    match_country,
    match_neutral,
    home_wins / total_matches AS home_win_percentage
FROM home_win



