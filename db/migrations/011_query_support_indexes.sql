CREATE INDEX IF NOT EXISTS idx_match_events_match_type_player
    ON match_events (match_id, event_type, player_id);

CREATE INDEX IF NOT EXISTS idx_match_events_match_type_secondary_player
    ON match_events (match_id, event_type, secondary_player_id);

CREATE INDEX IF NOT EXISTS idx_match_lineups_player_match
    ON match_lineups (player_id, match_id);
