import type postgres from 'postgres';
import type { TeamTranslationCandidateSourceType, TeamTranslationCandidateStatus } from './team-translation-candidates.mts';

interface UpsertCountryTranslationCandidateInput {
  countryCode: string;
  locale: string;
  proposedName: string;
  status: TeamTranslationCandidateStatus;
  sourceType: TeamTranslationCandidateSourceType;
  sourceUrl?: string | null;
  sourceLabel?: string | null;
  sourceRef?: string | null;
  notes?: string | null;
  reviewedAt?: string | Date | null;
  reviewedBy?: string | null;
}

export async function upsertCountryTranslationCandidate(
  sql: postgres.Sql,
  input: UpsertCountryTranslationCandidateInput,
) {
  await sql`
    INSERT INTO country_translation_candidates (
      country_id,
      locale,
      proposed_name,
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
      (SELECT id FROM countries WHERE code_alpha3 = ${input.countryCode}),
      ${input.locale},
      ${input.proposedName},
      ${input.status},
      ${input.sourceType},
      ${input.sourceUrl ?? null},
      ${input.sourceLabel ?? null},
      ${input.sourceRef ?? null},
      ${input.notes ?? null},
      ${input.reviewedAt ?? null},
      ${input.reviewedBy ?? null}
    )
    ON CONFLICT (country_id, locale, proposed_name_normalized, source_key)
    DO UPDATE SET
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
