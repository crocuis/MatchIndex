CREATE TABLE team_translation_candidates (
    id BIGSERIAL PRIMARY KEY,
    team_id BIGINT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    locale VARCHAR(10) NOT NULL REFERENCES locales(code),
    proposed_name VARCHAR(255) NOT NULL,
    proposed_short_name VARCHAR(50),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'quarantined')),
    source_type TEXT NOT NULL DEFAULT 'manual' CHECK (source_type IN ('manual', 'imported', 'merge_derived', 'historical_rule', 'machine_generated', 'legacy')),
    source_url TEXT,
    source_label TEXT,
    source_ref TEXT,
    notes TEXT,
    reviewed_at TIMESTAMPTZ,
    reviewed_by TEXT,
    promoted_at TIMESTAMPTZ,
    promoted_by TEXT,
    proposed_name_normalized VARCHAR(255) GENERATED ALWAYS AS (lower(proposed_name)) STORED,
    source_key TEXT GENERATED ALWAYS AS (COALESCE(source_url, '') || '|' || COALESCE(source_ref, '')) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_team_translation_candidates_unique ON team_translation_candidates (team_id, locale, proposed_name_normalized, source_key);
CREATE INDEX idx_team_translation_candidates_status ON team_translation_candidates (status, locale, team_id);
CREATE INDEX idx_team_translation_candidates_pending ON team_translation_candidates (locale, team_id, created_at DESC) WHERE status = 'pending';
CREATE INDEX idx_team_translation_candidates_approved ON team_translation_candidates (locale, team_id, reviewed_at DESC, created_at DESC) WHERE status = 'approved';
