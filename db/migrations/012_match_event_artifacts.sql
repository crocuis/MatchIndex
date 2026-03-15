CREATE TABLE match_event_artifacts (
    id BIGSERIAL PRIMARY KEY,
    match_id BIGINT NOT NULL,
    match_date DATE NOT NULL,
    artifact_type VARCHAR(40) NOT NULL,
    format VARCHAR(20) NOT NULL,
    storage_key TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    row_count INTEGER,
    byte_size BIGINT,
    checksum_sha256 CHAR(64),
    source_vendor VARCHAR(40),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (match_id, match_date) REFERENCES matches(id, match_date),
    CONSTRAINT match_event_artifacts_type_check CHECK (
        artifact_type IN ('analysis_detail', 'freeze_frames', 'visible_areas', 'raw_event_bundle')
    ),
    CONSTRAINT match_event_artifacts_format_check CHECK (format = 'json.gz'),
    UNIQUE (match_id, artifact_type, version)
);

CREATE INDEX idx_match_event_artifacts_match
    ON match_event_artifacts (match_id, artifact_type, version DESC);
