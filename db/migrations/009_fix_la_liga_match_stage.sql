UPDATE matches m
SET
    stage = 'REGULAR_SEASON',
    updated_at = NOW()
FROM competition_seasons cs
JOIN competitions c ON c.id = cs.competition_id
WHERE m.competition_season_id = cs.id
  AND c.slug = 'la-liga'
  AND m.stage = 'LEAGUE_PHASE';

REFRESH MATERIALIZED VIEW mv_team_form;
REFRESH MATERIALIZED VIEW mv_standings;
