
    
    

with dbt_test__target as (

  select game_id as unique_field
  from `checkmate-453316`.`chess_raw`.`games`
  where game_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


