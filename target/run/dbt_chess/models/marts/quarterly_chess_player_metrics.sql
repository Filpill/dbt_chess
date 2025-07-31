
   
      -- generated script to merge partitions into `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics`
      declare dbt_partitions_for_replacement array<date>;

      
      
       -- 1. create a temp table with model data
        
  
    

    create or replace table `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics__dbt_tmp`
      
    partition by quarter_start_date
    cluster by username, opening_archetype, time_class

    
    OPTIONS(
      description="""Aggregates player game statistics by quarter, time class, and opening archetype. This model provides breakdowns of game outcomes (win/loss/draw) and average accuracy by piece color (white/black) for rated standard chess games. It uses calendar and opening mapping tables to enrich the game data, and filters to only include recent games based on configurable date logic.\n""",
    
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      



WITH cte_date_aggregate AS (
      SELECT
            cal.quarter_start_date,
            cal.year_quarter,
            t.username,
            t.piece_color,
            t.time_class,
            COALESCE(map.opening_archetype, "Mapping Failed")         AS opening_archetype,
            AVG(t.accuracy)                                           AS avg_accuracy,
            SUM(CASE WHEN t.win_loss_draw = "win"  THEN 1 ELSE 0 END) AS win_count,
            SUM(CASE WHEN t.win_loss_draw = "loss" THEN 1 ELSE 0 END) AS loss_count,
            SUM(CASE WHEN t.win_loss_draw = "draw" THEN 1 ELSE 0 END) AS draw_count,
            COUNT(*)                                                  AS total_games
      FROM `checkmate-453316`.`dev_staging`.`stg__player_games` t
      LEFT JOIN `checkmate-453316`.`dev_universal`.`calendar` cal
        ON t.game_date = cal.cal_date
      LEFT JOIN `checkmate-453316`.`dev_universal`.`opening_mapping` map
          ON t.opening = map.opening
      WHERE 1=1
        
  
    AND t.game_date >= DATE_TRUNC(CURRENT_DATE - 40 , QUARTER)
  

        AND t.rated = TRUE
        AND t.rules = "chess"
      GROUP BY ALL
),

cte_pivot_piece_color AS (
    SELECT
        wagg.quarter_start_date,
        wagg.year_quarter,
        wagg.username,
        wagg.time_class,
        wagg.opening_archetype,
        SUM(wagg.total_games)                                                           AS total_games,

        SUM(CASE WHEN wagg.piece_color = "white" THEN wagg.win_count  ELSE 0 END)       AS white_win_count,
        SUM(CASE WHEN wagg.piece_color = "white" THEN wagg.loss_count ELSE 0 END)       AS white_loss_count,
        SUM(CASE WHEN wagg.piece_color = "white" THEN wagg.draw_count ELSE 0 END)       AS white_draw_count,
        AVG(CASE WHEN wagg.piece_color = "white" THEN wagg.avg_accuracy ELSE NULL END)  AS white_accuracy,

        SUM(CASE WHEN wagg.piece_color = "black" THEN wagg.win_count  ELSE 0 END)       AS black_win_count,
        SUM(CASE WHEN wagg.piece_color = "black" THEN wagg.loss_count ELSE 0 END)       AS black_loss_count,
        SUM(CASE WHEN wagg.piece_color = "black" THEN wagg.draw_count ELSE 0 END)       AS black_draw_count,
        AVG(CASE WHEN wagg.piece_color = "black" THEN wagg.avg_accuracy ELSE NULL END)  AS black_accuracy,

    FROM cte_date_aggregate wagg
    GROUP BY ALL
)

SELECT
      quarter_start_date,
      year_quarter,
      username,
      time_class,
      opening_archetype,
      total_games,
      white_win_count,
      white_loss_count,
      white_draw_count,
      white_accuracy,
      black_win_count,
      black_loss_count,
      black_draw_count,
      black_accuracy
FROM cte_pivot_piece_color
    );
  
      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
              array_agg(distinct date(quarter_start_date) IGNORE NULLS)
          from `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics` as DBT_INTERNAL_DEST
        using (
        select
        * from `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.quarter_start_date) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`quarter_start_date`, `year_quarter`, `username`, `time_class`, `opening_archetype`, `total_games`, `white_win_count`, `white_loss_count`, `white_draw_count`, `white_accuracy`, `black_win_count`, `black_loss_count`, `black_draw_count`, `black_accuracy`)
    values
        (`quarter_start_date`, `year_quarter`, `username`, `time_class`, `opening_archetype`, `total_games`, `white_win_count`, `white_loss_count`, `white_draw_count`, `white_accuracy`, `black_win_count`, `black_loss_count`, `black_draw_count`, `black_accuracy`)

;

      -- 4. clean up the temp table
      drop table if exists `checkmate-453316`.`dev_marts`.`quarterly_chess_player_metrics__dbt_tmp`

  


  

    