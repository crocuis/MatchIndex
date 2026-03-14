import type postgres from 'postgres';
import type { TeamTranslationCandidateSourceType, TeamTranslationCandidateStatus } from './team-translation-candidates.mts';

interface UpsertCompetitionTranslationCandidateInput {
  competitionSlug: string;
  locale: string;
  proposedName: string;
  proposedShortName?: string | null;
  status: TeamTranslationCandidateStatus;
  sourceType: TeamTranslationCandidateSourceType;
  sourceUrl?: string | null;
  sourceLabel?: string | null;
  sourceRef?: string | null;
  notes?: string | null;
  reviewedAt?: string | Date | null;
  reviewedBy?: string | null;
}

export async function upsertCompetitionTranslationCandidate(
  sql: postgres.Sql,
  input: UpsertCompetitionTranslationCandidateInput,
) {
  await sql`
    INSERT INTO competition_translation_candidates (
      competition_id,
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
      (SELECT id FROM competitions WHERE slug = ${input.competitionSlug}),
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
    ON CONFLICT (competition_id, locale, proposed_name_normalized, source_key)
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
