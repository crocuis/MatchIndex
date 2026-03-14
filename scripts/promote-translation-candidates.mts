import type postgres from 'postgres';

interface TeamCandidateRow {
  id: number;
  locale: string;
  proposed_name: string;
  proposed_short_name: string | null;
  team_id: number;
}

interface CompetitionCandidateRow {
  id: number;
  locale: string;
  proposed_name: string;
  proposed_short_name: string | null;
  competition_id: number;
}

interface CountryCandidateRow {
  id: number;
  locale: string;
  proposed_name: string;
  country_id: number;
}

export async function promoteApprovedTeamTranslationCandidates(sql: postgres.Sql, locale: string, promotedBy: string) {
  const candidates = await sql<TeamCandidateRow[]>`
    SELECT DISTINCT ON (ttc.team_id, ttc.locale)
      ttc.id,
      ttc.team_id,
      ttc.locale,
      ttc.proposed_name,
      ttc.proposed_short_name
    FROM team_translation_candidates ttc
    WHERE ttc.locale = ${locale}
      AND ttc.status = 'approved'
      AND ttc.promoted_at IS NULL
    ORDER BY
      ttc.team_id,
      ttc.locale,
      CASE ttc.source_type
        WHEN 'manual' THEN 5
        WHEN 'imported' THEN 4
        WHEN 'legacy' THEN 3
        WHEN 'merge_derived' THEN 2
        WHEN 'historical_rule' THEN 1
        ELSE 0
      END DESC,
      COALESCE(ttc.reviewed_at, ttc.created_at) DESC,
      ttc.created_at DESC,
      ttc.id DESC
  `;

  for (const candidate of candidates) {
    await sql`
      INSERT INTO team_translations (team_id, locale, name, short_name)
      VALUES (${candidate.team_id}, ${candidate.locale}, ${candidate.proposed_name}, ${candidate.proposed_short_name})
      ON CONFLICT (team_id, locale)
      DO UPDATE SET
        name = EXCLUDED.name,
        short_name = EXCLUDED.short_name
    `;

    await sql`
      UPDATE team_translation_candidates
      SET promoted_at = NOW(),
          promoted_by = ${promotedBy},
          updated_at = NOW()
      WHERE id = ${candidate.id}
    `;
  }

  return candidates.length;
}

export async function promoteApprovedCompetitionTranslationCandidates(sql: postgres.Sql, locale: string, promotedBy: string) {
  const candidates = await sql<CompetitionCandidateRow[]>`
    SELECT DISTINCT ON (ctc.competition_id, ctc.locale)
      ctc.id,
      ctc.competition_id,
      ctc.locale,
      ctc.proposed_name,
      ctc.proposed_short_name
    FROM competition_translation_candidates ctc
    WHERE ctc.locale = ${locale}
      AND ctc.status = 'approved'
      AND ctc.promoted_at IS NULL
    ORDER BY
      ctc.competition_id,
      ctc.locale,
      CASE ctc.source_type
        WHEN 'manual' THEN 5
        WHEN 'imported' THEN 4
        WHEN 'legacy' THEN 3
        WHEN 'merge_derived' THEN 2
        WHEN 'historical_rule' THEN 1
        ELSE 0
      END DESC,
      COALESCE(ctc.reviewed_at, ctc.created_at) DESC,
      ctc.created_at DESC,
      ctc.id DESC
  `;

  for (const candidate of candidates) {
    await sql`
      INSERT INTO competition_translations (competition_id, locale, name, short_name)
      VALUES (${candidate.competition_id}, ${candidate.locale}, ${candidate.proposed_name}, ${candidate.proposed_short_name})
      ON CONFLICT (competition_id, locale)
      DO UPDATE SET
        name = EXCLUDED.name,
        short_name = EXCLUDED.short_name
    `;

    await sql`
      UPDATE competition_translation_candidates
      SET promoted_at = NOW(),
          promoted_by = ${promotedBy},
          updated_at = NOW()
      WHERE id = ${candidate.id}
    `;
  }

  return candidates.length;
}

export async function promoteApprovedCountryTranslationCandidates(sql: postgres.Sql, locale: string, promotedBy: string) {
  const candidates = await sql<CountryCandidateRow[]>`
    SELECT DISTINCT ON (ctc.country_id, ctc.locale)
      ctc.id,
      ctc.country_id,
      ctc.locale,
      ctc.proposed_name
    FROM country_translation_candidates ctc
    WHERE ctc.locale = ${locale}
      AND ctc.status = 'approved'
      AND ctc.promoted_at IS NULL
    ORDER BY
      ctc.country_id,
      ctc.locale,
      CASE ctc.source_type
        WHEN 'manual' THEN 5
        WHEN 'imported' THEN 4
        WHEN 'legacy' THEN 3
        WHEN 'merge_derived' THEN 2
        WHEN 'historical_rule' THEN 1
        ELSE 0
      END DESC,
      COALESCE(ctc.reviewed_at, ctc.created_at) DESC,
      ctc.created_at DESC,
      ctc.id DESC
  `;

  for (const candidate of candidates) {
    await sql`
      INSERT INTO country_translations (country_id, locale, name)
      VALUES (${candidate.country_id}, ${candidate.locale}, ${candidate.proposed_name})
      ON CONFLICT (country_id, locale)
      DO UPDATE SET name = EXCLUDED.name
    `;

    await sql`
      UPDATE country_translation_candidates
      SET promoted_at = NOW(),
          promoted_by = ${promotedBy},
          updated_at = NOW()
      WHERE id = ${candidate.id}
    `;
  }

  return candidates.length;
}
