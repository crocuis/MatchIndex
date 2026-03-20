import postgres from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';
import { deriveReviewedTeamShortName } from './team-short-name-policy.mts';
import { upsertTeamTranslationCandidate } from './team-translation-candidates.mts';

interface CliOptions {
  dryRun: boolean;
  includeDifferent: boolean;
  limit: number | null;
  locale: string;
  prepareOnly: boolean;
  scope: 'all' | 'latest';
}

interface TeamTargetRow {
  slug: string;
  en_name: string;
  ko_name: string | null;
  ko_short_name: string | null;
}

interface DeepLTranslationResponse {
  translations: Array<{
    detected_source_language?: string;
    text: string;
  }>;
}

interface TeamCandidatePreview {
  slug: string;
  englishName: string;
  currentKoName: string | null;
  proposedKoName: string | null;
  proposedKoShortName: string | null;
  reason: 'different' | 'english_fallback' | 'missing';
}

const TARGET_LOCALE = 'ko';
const SOURCE_REF = 'scripts/import-deepl-team-ko-candidates.mts';
const SOURCE_LABEL = 'DeepL imported translation draft';
const SOURCE_URL = 'https://developers.deepl.com/api-reference/translate';
const DEFAULT_CONTEXT = 'Translate football club names into Korean. Keep club abbreviations such as FC, AFC, AC, AS, PSV, and WFC when they are part of the official name. Return only the Korean club name.';

function getArgValue(argv: string[], key: string) {
  return argv.find((arg) => arg.startsWith(`${key}=`))?.slice(key.length + 1) ?? null;
}

function parseArgs(argv: string[]): CliOptions {
  const limitRaw = getArgValue(argv, '--limit');
  const parsedLimit = limitRaw ? Number.parseInt(limitRaw, 10) : null;
  const scope = getArgValue(argv, '--scope');

  return {
    dryRun: !argv.includes('--write'),
    includeDifferent: argv.includes('--include-different'),
    limit: Number.isFinite(parsedLimit) && parsedLimit !== null && parsedLimit > 0 ? parsedLimit : null,
    locale: getArgValue(argv, '--locale') ?? TARGET_LOCALE,
    prepareOnly: argv.includes('--prepare-only'),
    scope: scope === 'all' ? 'all' : 'latest',
  };
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, { max: 1, prepare: false, idle_timeout: 5 });
}

function hasEnglishFallback(localized: string | null, english: string) {
  const trimmedLocalized = localized?.trim();
  const trimmedEnglish = english.trim();
  if (!trimmedLocalized) {
    return false;
  }

  return /[A-Za-z]/.test(trimmedLocalized) && trimmedLocalized.toLowerCase() === trimmedEnglish.toLowerCase();
}

function containsHangul(value: string | null | undefined) {
  return /[가-힣]/.test(value ?? '');
}

function isAcceptedUnchangedAbbreviation(value: string) {
  const normalized = value.trim();
  return /^[A-Z0-9]{2,8}$/.test(normalized);
}

function getReason(currentKoName: string | null, englishName: string) {
  if (!currentKoName?.trim()) {
    return 'missing' as const;
  }

  if (hasEnglishFallback(currentKoName, englishName)) {
    return 'english_fallback' as const;
  }

  return 'different' as const;
}

function shouldKeepDeepLCandidate(englishName: string, translatedName: string, currentKoName: string | null, includeDifferent: boolean) {
  const normalizedEnglish = englishName.trim();
  const normalizedTranslated = translatedName.trim();
  if (!normalizedTranslated) {
    return false;
  }

  const isUnchanged = normalizedTranslated.toLowerCase() === normalizedEnglish.toLowerCase();
  if (isUnchanged && !isAcceptedUnchangedAbbreviation(normalizedTranslated)) {
    return false;
  }

  if (!containsHangul(normalizedTranslated) && !isAcceptedUnchangedAbbreviation(normalizedTranslated)) {
    return false;
  }

  if (!includeDifferent) {
    return true;
  }

  return normalizedTranslated !== (currentKoName?.trim() ?? '');
}

async function getTargets(sql: postgres.Sql, options: CliOptions) {
  const limitSql = options.limit ? sql`LIMIT ${options.limit}` : sql``;

  const latestSeasonClause = options.scope === 'latest'
    ? sql`
        WITH target_teams AS (
          WITH latest_team_seasons AS (
            SELECT DISTINCT ON (ts.team_id)
              ts.team_id
            FROM team_seasons ts
            JOIN competition_seasons cs ON cs.id = ts.competition_season_id
            JOIN seasons s ON s.id = cs.season_id
            ORDER BY ts.team_id, s.end_date DESC NULLS LAST, s.start_date DESC NULLS LAST, cs.id DESC
          )
          SELECT t.id, t.slug
          FROM teams t
          JOIN latest_team_seasons lts ON lts.team_id = t.id
          WHERE t.is_national = FALSE
        )
      `
    : sql`
        WITH target_teams AS (
          SELECT t.id, t.slug
          FROM teams t
          WHERE t.is_national = FALSE
            AND t.is_active = TRUE
        )
      `;

  const rows = await sql<TeamTargetRow[]>`
    ${latestSeasonClause}
    SELECT
      target_teams.slug,
      COALESCE(en.name, target_teams.slug) AS en_name,
      ko.name AS ko_name,
      ko.short_name AS ko_short_name
    FROM target_teams
    LEFT JOIN team_translations en ON en.team_id = target_teams.id AND en.locale = 'en'
    LEFT JOIN team_translations ko ON ko.team_id = target_teams.id AND ko.locale = ${options.locale}
    LEFT JOIN team_translation_candidates pending
      ON pending.team_id = target_teams.id
      AND pending.locale = ${options.locale}
      AND pending.status IN ('pending', 'approved')
      AND pending.source_ref = ${SOURCE_REF}
    WHERE pending.id IS NULL
      AND (
        ko.name IS NULL
        OR (ko.name ~ '[A-Za-z]' AND lower(ko.name) = lower(COALESCE(en.name, target_teams.slug)))
        OR ${options.includeDifferent}
      )
    ORDER BY target_teams.slug ASC
    ${limitSql}
  `;

  if (!options.includeDifferent) {
    return rows.filter((row) => !row.ko_name?.trim() || hasEnglishFallback(row.ko_name, row.en_name));
  }

  return rows;
}

async function translateWithDeepL(authKey: string, names: string[]) {
  const apiUrl = authKey.endsWith(':fx') ? 'https://api-free.deepl.com/v2/translate' : 'https://api.deepl.com/v2/translate';
  const payload = new URLSearchParams();
  payload.set('source_lang', 'EN');
  payload.set('target_lang', 'KO');
  payload.set('context', DEFAULT_CONTEXT);
  for (const name of names) {
    payload.append('text', name);
  }

  const response = await fetch(apiUrl, {
    method: 'POST',
    headers: {
      Authorization: `DeepL-Auth-Key ${authKey}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: payload.toString(),
  });

  if (!response.ok) {
    throw new Error(`DeepL request failed: HTTP ${response.status}`);
  }

  const json = await response.json() as DeepLTranslationResponse;
  if (json.translations.length !== names.length) {
    throw new Error(`DeepL returned ${json.translations.length} translations for ${names.length} names`);
  }

  return json.translations.map((translation) => translation.text.trim());
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));
  const sql = getSql();

  try {
    const targets = await getTargets(sql, options);
    if (targets.length === 0) {
      console.log(JSON.stringify({
        dryRun: options.dryRun,
        prepareOnly: options.prepareOnly,
        includeDifferent: options.includeDifferent,
        locale: options.locale,
        scope: options.scope,
        targetCount: 0,
        insertedCount: 0,
        preview: [],
      }, null, 2));
      return;
    }

    if (options.prepareOnly) {
      console.log(JSON.stringify({
        dryRun: options.dryRun,
        prepareOnly: true,
        includeDifferent: options.includeDifferent,
        locale: options.locale,
        scope: options.scope,
        targetCount: targets.length,
        insertedCount: 0,
        preview: targets.slice(0, 25).map((target) => ({
          slug: target.slug,
          englishName: target.en_name,
          currentKoName: target.ko_name,
          proposedKoName: null,
          proposedKoShortName: null,
          reason: !target.ko_name?.trim() ? 'missing' : hasEnglishFallback(target.ko_name, target.en_name) ? 'english_fallback' : 'different',
        } satisfies TeamCandidatePreview)),
      }, null, 2));
      return;
    }

    const authKey = process.env.DEEPL_API_KEY ?? process.env.DEEPL_AUTH_KEY;
    if (!authKey) {
      throw new Error('DEEPL_API_KEY or DEEPL_AUTH_KEY is not set. Use --prepare-only to inspect targets without translating.');
    }

    const translations = await translateWithDeepL(authKey, targets.map((target) => target.en_name));
    const preview = targets.flatMap((target, index) => {
      const proposedKoName = translations[index];
      if (!shouldKeepDeepLCandidate(target.en_name, proposedKoName, target.ko_name, options.includeDifferent)) {
        return [];
      }

      return [{
        slug: target.slug,
        englishName: target.en_name,
        currentKoName: target.ko_name,
        proposedKoName,
        proposedKoShortName: deriveReviewedTeamShortName(proposedKoName),
        reason: getReason(target.ko_name, target.en_name),
      } satisfies TeamCandidatePreview];
    });

    if (options.dryRun) {
      console.log(JSON.stringify({
        dryRun: true,
        prepareOnly: false,
        includeDifferent: options.includeDifferent,
        locale: options.locale,
        scope: options.scope,
        targetCount: targets.length,
        insertedCount: 0,
        preview: preview.slice(0, 50),
      }, null, 2));
      return;
    }

    for (const candidate of preview) {
      await upsertTeamTranslationCandidate(sql, {
        locale: options.locale,
        proposedName: candidate.proposedKoName ?? candidate.englishName,
        proposedShortName: candidate.proposedKoShortName,
        reviewedAt: null,
        reviewedBy: null,
        sourceLabel: SOURCE_LABEL,
        sourceRef: SOURCE_REF,
        sourceType: 'imported',
        sourceUrl: SOURCE_URL,
        status: 'pending',
        teamSlug: candidate.slug,
        notes: `DeepL draft for ${candidate.reason}`,
      });
    }

    console.log(JSON.stringify({
      dryRun: false,
      prepareOnly: false,
      includeDifferent: options.includeDifferent,
      locale: options.locale,
      scope: options.scope,
      targetCount: targets.length,
      insertedCount: preview.length,
      preview: preview.slice(0, 50),
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
