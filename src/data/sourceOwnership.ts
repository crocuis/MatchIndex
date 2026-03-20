import type { Sql } from 'postgres';

export type CompetitionSeasonWriteTarget = 'matchArtifacts' | 'matchStats' | 'playerContracts' | 'playerSeasonStats';
export type CompetitionSeasonWriteMode = 'backfill' | 'sync';

interface CompetitionSeasonPolicyRow {
  id: number;
  source_metadata: unknown;
}

interface CompetitionSeasonPolicyMetadata {
  backfillAllowedSources?: string[];
  freezeReason?: string | null;
  frozenAt?: string | null;
  owners?: Partial<Record<CompetitionSeasonWriteTarget, string>>;
  preferredArtifactSources?: string[];
}

function toMetadataObject(value: unknown) {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? { ...(value as Record<string, unknown>) }
    : {};
}

export interface CompetitionSeasonPolicy {
  backfillAllowedSources: string[];
  freezeReason: string | null;
  frozenAt: string | null;
  owners: Partial<Record<CompetitionSeasonWriteTarget, string>>;
  preferredArtifactSources: string[];
}

const DEFAULT_POLICY: CompetitionSeasonPolicy = {
  backfillAllowedSources: [],
  freezeReason: null,
  frozenAt: null,
  owners: {},
  preferredArtifactSources: [],
};

function normalizeSource(source: string) {
  return source.trim().toLowerCase();
}

export function parseCompetitionSeasonPolicy(value: unknown): CompetitionSeasonPolicy {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return DEFAULT_POLICY;
  }

  const metadata = value as CompetitionSeasonPolicyMetadata;

  return {
    backfillAllowedSources: (metadata.backfillAllowedSources ?? []).map(normalizeSource),
    freezeReason: metadata.freezeReason ?? null,
    frozenAt: metadata.frozenAt ?? null,
    owners: metadata.owners ?? {},
    preferredArtifactSources: (metadata.preferredArtifactSources ?? []).map(normalizeSource),
  };
}

export async function loadCompetitionSeasonPolicies(sql: Sql, competitionSeasonIds: number[]) {
  if (competitionSeasonIds.length === 0) {
    return new Map<number, CompetitionSeasonPolicy>();
  }

  const rows = await sql<CompetitionSeasonPolicyRow[]>`
    SELECT id, source_metadata
    FROM competition_seasons
    WHERE id = ANY(${competitionSeasonIds})
  `;

  return new Map(rows.map((row) => [row.id, parseCompetitionSeasonPolicy(row.source_metadata)]));
}

export async function updateCompetitionSeasonPolicy(
  sql: Sql,
  competitionCode: string,
  seasonStartYear: number,
  patch: Partial<CompetitionSeasonPolicy>,
  dryRun: boolean = false,
) {
  const rows = await sql<CompetitionSeasonPolicyRow[]>`
    SELECT cs.id, cs.source_metadata
    FROM competition_seasons cs
    JOIN competitions c ON c.id = cs.competition_id
    JOIN seasons s ON s.id = cs.season_id
    WHERE LOWER(c.code) = LOWER(${competitionCode})
      AND EXTRACT(YEAR FROM s.start_date)::INT = ${seasonStartYear}
    LIMIT 1
  `;

  if (!rows[0]) {
    throw new Error(`competition season not found: ${competitionCode} ${seasonStartYear}`);
  }

  const current = parseCompetitionSeasonPolicy(rows[0].source_metadata);
  const currentMetadataObject = toMetadataObject(rows[0].source_metadata);
  const merged: CompetitionSeasonPolicy = {
    backfillAllowedSources: patch.backfillAllowedSources ?? current.backfillAllowedSources,
    freezeReason: patch.freezeReason ?? current.freezeReason,
    frozenAt: patch.frozenAt ?? current.frozenAt,
    owners: patch.owners ? { ...current.owners, ...patch.owners } : current.owners,
    preferredArtifactSources: patch.preferredArtifactSources ?? current.preferredArtifactSources,
  };

  if (!dryRun) {
    const nextMetadata = {
      ...currentMetadataObject,
      backfillAllowedSources: merged.backfillAllowedSources,
      freezeReason: merged.freezeReason,
      frozenAt: merged.frozenAt,
      owners: merged.owners,
      preferredArtifactSources: merged.preferredArtifactSources,
    };

    await sql`
      UPDATE competition_seasons
      SET source_metadata = ${sql.json(nextMetadata)},
          updated_at = NOW()
      WHERE id = ${rows[0].id}
    `;
  }

  return { competitionSeasonId: rows[0].id, policy: merged };
}

export function isCompetitionSeasonWriteAllowed(
  policy: CompetitionSeasonPolicy | undefined,
  target: CompetitionSeasonWriteTarget,
  sourceSlug: string,
  mode: CompetitionSeasonWriteMode,
) {
  if (!policy) {
    return true;
  }

  const normalizedSource = normalizeSource(sourceSlug);
  const owner = policy.owners[target]?.trim().toLowerCase() ?? null;

  if (policy.frozenAt) {
    if (mode === 'sync') {
      return false;
    }

    return owner === null || owner === normalizedSource || policy.backfillAllowedSources.includes(normalizedSource);
  }

  if (!owner) {
    return mode === 'backfill'
      ? policy.backfillAllowedSources.length === 0 || policy.backfillAllowedSources.includes(normalizedSource)
      : true;
  }

  if (owner === normalizedSource) {
    return true;
  }

  return mode === 'backfill' && policy.backfillAllowedSources.includes(normalizedSource);
}
