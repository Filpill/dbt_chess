
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    COUNT(*) AS raw_count
FROM `checkmate-453316`.`chess_raw`.`games`
WHERE 1=1
  AND game_date >= DATE_TRUNC(DATE('2025-05-01'), QUARTER)
  AND rated = TRUE
  AND rules = "chess"

EXCEPT DISTINCT

SELECT
    CAST(SUM(total_games)/2 AS INT64) AS agg_count
FROM `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics`
WHERE 1=1
  AND quarter_start_date >= DATE_TRUNC(DATE('2025-05-01'), QUARTER)
  
  
      
    ) dbt_internal_test