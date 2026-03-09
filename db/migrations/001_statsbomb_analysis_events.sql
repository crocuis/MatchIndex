ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'pass';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'shot';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'carry';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'pressure';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'ball_receipt';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'clearance';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'interception';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'block';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'ball_recovery';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'foul_won';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'foul_committed';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'duel';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'miscontrol';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'goalkeeper';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'offside';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'dribble';
ALTER TYPE match_event_type ADD VALUE IF NOT EXISTS 'dispossessed';

ALTER TABLE match_events
  ADD COLUMN IF NOT EXISTS is_notable BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE match_events
SET is_notable = TRUE
WHERE event_type IN (
  'goal',
  'own_goal',
  'penalty_scored',
  'penalty_missed',
  'yellow_card',
  'red_card',
  'yellow_red_card',
  'substitution',
  'var_decision'
);

CREATE INDEX IF NOT EXISTS idx_match_events_analysis
  ON match_events (match_id, event_type)
  WHERE is_notable = FALSE;

CREATE INDEX IF NOT EXISTS idx_match_events_notable
  ON match_events (match_id, minute, extra_minute)
  WHERE is_notable = TRUE;
