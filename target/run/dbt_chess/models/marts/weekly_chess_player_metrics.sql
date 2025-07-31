
   
      -- generated script to merge partitions into `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics`
      declare dbt_partitions_for_replacement array<date>;

      
      
       -- 1. create a temp table with model data
        
  
    

    create or replace table `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics__dbt_tmp`
      
    partition by week_start_date
    cluster by username, time_class

    
    OPTIONS(
      description="""Weekly aggregate statistics of rated standard chess games by player, piece color, and time class. This model calculates performance metrics such as win/loss/draw counts, accuracy, and rating, separated by white and black games. It uses the calendar table to align games to ISO week start dates and is designed for incremental refresh with weekly partitioning and clustering.\n""",
    
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      



WITH cte_date_aggregate AS (
      SELECT
            cal.iso_week_start_date                                   AS week_start_date,
            cal.iso_week_desc                                         AS week_number,
            t.username,
            t.piece_color,
            t.time_class,
            AVG(t.rating)                                             AS avg_rating,
            AVG(t.accuracy)                                           AS avg_accuracy,
            SUM(CASE WHEN t.win_loss_draw = "win"  THEN 1 ELSE 0 END) AS win_count,
            SUM(CASE WHEN t.win_loss_draw = "loss" THEN 1 ELSE 0 END) AS loss_count,
            SUM(CASE WHEN t.win_loss_draw = "draw" THEN 1 ELSE 0 END) AS draw_count,
            COUNT(*)                                                  AS total_games
      FROM `checkmate-453316`.`dev_staging`.`stg__player_games` t
      LEFT JOIN `checkmate-453316`.`dev_universal`.`calendar` cal
        ON t.game_date = cal.cal_date
      WHERE 1=1
        
  
    AND t.game_date >= DATE_TRUNC(CURRENT_DATE - 40 , ISOWEEK)
  

        AND t.rated = TRUE
        AND t.rules = "chess"
      GROUP BY ALL
),

cte_pivot_piece_color AS (
    SELECT
        wagg.week_start_date,
        wagg.week_number,
        wagg.username,
        wagg.time_class,
        AVG(wagg.avg_rating)                                                            AS avg_rating,
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
      week_start_date,
      week_number,
      username,
      time_class,
      avg_rating,
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
              array_agg(distinct date(week_start_date) IGNORE NULLS)
          from `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics` as DBT_INTERNAL_DEST
        using (
        select
        * from `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.week_start_date) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`week_start_date`, `week_number`, `username`, `time_class`, `avg_rating`, `total_games`, `white_win_count`, `white_loss_count`, `white_draw_count`, `white_accuracy`, `black_win_count`, `black_loss_count`, `black_draw_count`, `black_accuracy`)
    values
        (`week_start_date`, `week_number`, `username`, `time_class`, `avg_rating`, `total_games`, `white_win_count`, `white_loss_count`, `white_draw_count`, `white_accuracy`, `black_win_count`, `black_loss_count`, `black_draw_count`, `black_accuracy`)

;

      -- 4. clean up the temp table
      drop table if exists `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics__dbt_tmp`

  


  

    