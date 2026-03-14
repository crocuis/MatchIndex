import type postgres from 'postgres';

export type TeamTranslationCandidateStatus = 'pending' | 'approved' | 'rejected' | 'quarantined';
export type TeamTranslationCandidateSourceType = 'manual' | 'imported' | 'merge_derived' | 'historical_rule' | 'machine_generated' | 'legacy';

interface UpsertTeamTranslationCandidateInput {
  locale: string;
  proposedName: string;
  proposedShortName?: string | null;
  reviewedAt?: string | Date | null;
  reviewedBy?: string | null;
  sourceLabel?: string | null;
  sourceRef?: string | null;
  sourceType: TeamTranslationCandidateSourceType;
  sourceUrl?: string | null;
  status: TeamTranslationCandidateStatus;
  teamSlug: string;
  notes?: string | null;
}

export async function upsertTeamTranslationCandidate(
  sql: postgres.Sql,
  input: UpsertTeamTranslationCandidateInput,
) {
  await sql`
    INSERT INTO team_translation_candidates (
      team_id,
      locale,
      proposed_name,
      proposed_short_name,
      status,
      source_type,
      source_url,
      source_label,
      source_ref,
      notes,
      reviewed_at,
      reviewed_by
    )
    VALUES (
      (SELECT id FROM teams WHERE slug = ${input.teamSlug}),
      ${input.locale},
      ${input.proposedName},
      ${input.proposedShortName ?? null},
      ${input.status},
      ${input.sourceType},
      ${input.sourceUrl ?? null},
      ${input.sourceLabel ?? null},
      ${input.sourceRef ?? null},
      ${input.notes ?? null},
      ${input.reviewedAt ?? null},
      ${input.reviewedBy ?? null}
    )
    ON CONFLICT (team_id, locale, proposed_name_normalized, source_key)
    DO UPDATE SET
      proposed_short_name = EXCLUDED.proposed_short_name,
      status = EXCLUDED.status,
      source_type = EXCLUDED.source_type,
      source_url = EXCLUDED.source_url,
      source_label = EXCLUDED.source_label,
      source_ref = EXCLUDED.source_ref,
      notes = EXCLUDED.notes,
      reviewed_at = EXCLUDED.reviewed_at,
      reviewed_by = EXCLUDED.reviewed_by,
      updated_at = NOW()
  `;
}
