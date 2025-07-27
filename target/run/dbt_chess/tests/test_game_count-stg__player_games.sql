
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT
    COUNT(*) AS raw_count
FROM `checkmate-453316`.`chess_raw`.`games`
WHERE 1=1
  AND game_date >= DATE('2025-05-01')

EXCEPT DISTINCT

SELECT
    CAST(COUNT(*)/2 AS INT64) AS agg_count
FROM `checkmate-453316`.`dev_staging`.`stg__player_games`
WHERE 1=1
  AND game_date >= DATE('2025-05-01')
  
  
      
    ) dbt_internal_test