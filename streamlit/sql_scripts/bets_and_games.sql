/*
Bets and games query
*/


select o.*,
  m.* EXCEPT (game_id)
from `odds-tracker-402301.nfl_data.daily_odds_data` o
join `odds-tracker-402301.nfl_data.game_metadata` m on m.game_id = o.game_id
order by game_date;
