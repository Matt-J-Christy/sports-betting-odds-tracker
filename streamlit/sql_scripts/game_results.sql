/*
Game results query
*/


select m.*,
    r.* EXCEPT (game_id, home_team_id, away_team_id),

from `odds-tracker-402301.nfl_data.game_metadata` m
join `odds-tracker-402301.nfl_data.game_results` r on r.game_id = m.game_id
order by week, game_date, m.home_team_id, m.away_team_id;
