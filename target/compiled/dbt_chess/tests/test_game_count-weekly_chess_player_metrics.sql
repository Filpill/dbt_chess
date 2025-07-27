SELECT
    COUNT(*) AS raw_count
FROM `checkmate-453316`.`chess_raw`.`games`
WHERE 1=1
  AND game_date >= DATE_TRUNC(DATE('2025-05-01'), ISOWEEK)
  AND rated = TRUE
  AND rules = "chess"

EXCEPT DISTINCT

SELECT
    CAST(SUM(total_games)/2 AS INT64) AS agg_count
FROM `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics`
WHERE 1=1
  AND week_start_date >= DATE_TRUNC(DATE('2025-05-01'), ISOWEEK)