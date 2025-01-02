STALE_PLAYER_INFO = '''
select 
  pi.* 
from 
  current_player_info pi 
  join (
    select 
      distinct playerid 
    from 
      skater_stats 
    where 
      year = (
        select 
          case when date_part('month', CURRENT_DATE) between 8 
          and 12 then concat(
            date_part('year', CURRENT_DATE), 
            '-', 
            date_part('year', CURRENT_DATE)+ 1
          ) else concat(
            date_part('year', CURRENT_DATE)-1, 
            '-', 
            date_part('year', CURRENT_DATE)
          ) end
      )
  ) curr on pi.playerid = curr.playerid 
where 
  load_date < CURRENT_DATE - INTERVAL '1 month' 
order by 
  draft_year_eligible desc nulls last, 
  date_of_birth desc nulls last
'''