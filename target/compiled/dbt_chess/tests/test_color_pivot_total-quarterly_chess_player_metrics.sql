SELECT
  week_start_date,
  username,
  time_class
FROM `checkmate-453316`.`dev_marts`.`weekly_chess_player_metrics`
WHERE 1=1
  AND week_start_date >= DATE_TRUNC(DATE('2025-05-01'), QUARTER)
  AND total_games != (white_win_count + white_loss_count + white_draw_count + black_win_count + black_loss_count + black_draw_count)