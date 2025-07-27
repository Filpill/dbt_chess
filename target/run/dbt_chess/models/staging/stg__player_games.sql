
   
      -- generated script to merge partitions into `checkmate-453316`.`dev_staging`.`stg__player_games`
      declare dbt_partitions_for_replacement array<date>;

      
      
       -- 1. create a temp table with model data
        
  
    

    create or replace table `checkmate-453316`.`dev_staging`.`stg__player_games__dbt_tmp`
      
    partition by game_date
    

    
    OPTIONS(
      description="""Stage model that transforms raw chess game data into a player-level view,  with one row per player per game (both white and black perspectives).  Includes standardizations for result classification (win/loss/draw),  basic game metadata, and parsed opening names. This model supports  incremental loads by game date for efficient backfills.\n""",
    
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      



WITH cte_white_black_union AS (

    SELECT
        game_id,
        game_date,
        "white"                                                AS piece_color,
        white.username                                         AS username,
        white.rating                                           AS rating,
        white.result                                           AS result,
        rated,
        time_class,
        time_control,
        rules,
        accuracies.white                                       AS accuracy,
        opening                                                AS opening_line,
    FROM `checkmate-453316`.`chess_raw`.`games` 

      UNION ALL

    SELECT
        game_id,
        game_date,
        "black"                                                AS piece_color,
        black.username                                         AS username,
        black.rating                                           AS rating,
        black.result                                           AS result,
        rated,
        time_class,
        time_control,
        rules,
        accuracies.black                                       AS accuracy,
        opening                                                AS opening_line,
    FROM `checkmate-453316`.`chess_raw`.`games`
)

SELECT
    t.game_id,
    t.game_date,
    t.username,
    t.rating,
    t.piece_color,
    t.time_class,
    t.rules,
    t.result                                               AS raw_result,
    t.rated,
    CASE
        WHEN t.result = "win"                 THEN "win"
        WHEN t.result = "timeout"             THEN "loss"
        WHEN t.result = "threecheck"          THEN "loss"
        WHEN t.result = "resigned"            THEN "loss"
        WHEN t.result = "kingofthehill"       THEN "loss"
        WHEN t.result = "checkmated"          THEN "loss"
        WHEN t.result = "bughousepartnerlose" THEN "loss"
        WHEN t.result = "abandoned"           THEN "loss"
        WHEN t.result = "timevsinsufficient"  THEN "draw"
        WHEN t.result = "stalemate"           THEN "draw"
        WHEN t.result = "repetition"          THEN "draw"
        WHEN t.result = "insufficient"        THEN "draw"
        WHEN t.result = "agreed"              THEN "draw"
        WHEN t.result = "50move"              THEN "draw"
    END                                                   AS win_loss_draw,
    t.opening_line                                        AS opening_line,
    TRIM(
        REGEXP_REPLACE(REGEXP_REPLACE(t.opening_line , r'\d.*$', ''), r'\.{3,}\s*$', '')
    )                                                     AS opening,
    t.accuracy,
FROM cte_white_black_union t

WHERE 1=1 
  
  
    AND t.game_date BETWEEN CURRENT_DATE() - 30 AND CURRENT_DATE()
  

    );
  
      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
              array_agg(distinct date(game_date) IGNORE NULLS)
          from `checkmate-453316`.`dev_staging`.`stg__player_games__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `checkmate-453316`.`dev_staging`.`stg__player_games` as DBT_INTERNAL_DEST
        using (
        select
        * from `checkmate-453316`.`dev_staging`.`stg__player_games__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.game_date) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`game_id`, `game_date`, `username`, `rating`, `piece_color`, `time_class`, `rules`, `raw_result`, `rated`, `win_loss_draw`, `opening_line`, `opening`, `accuracy`)
    values
        (`game_id`, `game_date`, `username`, `rating`, `piece_color`, `time_class`, `rules`, `raw_result`, `rated`, `win_loss_draw`, `opening_line`, `opening`, `accuracy`)

;

      -- 4. clean up the temp table
      drop table if exists `checkmate-453316`.`dev_staging`.`stg__player_games__dbt_tmp`

  


  

    