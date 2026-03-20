import { readFile } from 'node:fs/promises';
import path from 'node:path';
import postgres, { type Sql } from 'postgres';
import type { CountryCodeResolver } from '../src/data/countryCodeResolver.ts';
import { loadCountryCodeResolver } from '../src/data/countryCodeResolver.ts';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  competitionSlug?: string;
  dryRun: boolean;
  help: boolean;
  inputPath?: string;
  limit?: number;
  playerSlug?: string;
  seasonSlug?: string;
  teamSlug?: string;
}

interface ContractSyncPayload {
  provider: string;
  competition?: string;
  currencyCode?: string;
  fetchedAt?: string;
  rows: ContractSyncRow[];
  season?: string;
  sourceUrl?: string;
}

interface ContractSyncRow {
  age?: number | string | null;
  annualSalary?: number | string | null;
  dateOfBirth?: string | null;
  contractEndDate?: string | null;
  contractStartDate?: string | null;
  currencyCode?: string | null;
  heightCm?: number | string | null;
  isEstimated?: boolean | null;
  marketValue?: number | string | null;
  nationalities?: string[] | null;
  playerName?: string | null;
  playerSlug?: string | null;
  photoUrl?: string | null;
  position?: string | null;
  preferredFoot?: string | null;
  raw?: unknown;
  sourceUrl?: string | null;
  teamName?: string | null;
  weeklyWage?: number | string | null;
}

interface SyncTargetRow {
  competition_season_id: number;
  contract_id: number;
  full_name: string | null;
  known_as: string;
  player_slug: string;
  short_name: string | null;
  team_name: string;
  team_slug: string;
}

interface SyncTarget {
  competitionSeasonId: number;
  contractId: number;
  playerKeys: string[];
  playerSlug: string;
  teamKeys: string[];
}

interface SyncSummary {
  matched: number;
  unmatched: number;
  updated: number;
  skipped: number;
  provider: string;
}

type SummaryPayload = Record<string, unknown>;

interface SourceRow {
  id: number;
}

interface SyncRunRow {
  id: number;
}

interface CountryLookupRow {
  id: number;
  code_alpha2: string | null;
  code_alpha3: string;
  translation_name: string | null;
}

const COUNTRY_NAME_ALIASES: Record<string, string> = {
  'bosnia herzegovina': 'BIH',
  'cape verde': 'CPV',
  'congo dr': 'COD',
  'cote d ivoire': 'CIV',
  'curacao': 'CUW',
  'dr congo': 'COD',
  england: 'ENG',
  'ivory coast': 'CIV',
  'korea republic': 'KOR',
  'korea south': 'KOR',
  'north ireland': 'NIR',
  'north macedonia': 'MKD',
  'republic of ireland': 'IRL',
  scotland: 'SCO',
  'south korea': 'KOR',
  syria: 'SYR',
  'the gambia': 'GAM',
  'trinidad tobago': 'TRI',
  'u s a': 'USA',
  'united states': 'USA',
  'united states of america': 'USA',
  wales: 'WAL',
};

function parsePositiveInt(value: string | undefined) {
  if (!value) {
    return undefined;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {
    dryRun: false,
    help: false,
  };

  for (const arg of argv) {
    if (arg === '--dry-run') {
      options.dryRun = true;
      continue;
    }

    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }

    if (arg.startsWith('--input=')) {
      options.inputPath = arg.slice('--input='.length).trim();
      continue;
    }

    if (arg.startsWith('--competition=')) {
      options.competitionSlug = arg.slice('--competition='.length).trim();
      continue;
    }

    if (arg.startsWith('--season=')) {
      options.seasonSlug = arg.slice('--season='.length).trim();
      continue;
    }

    if (arg.startsWith('--player=')) {
      options.playerSlug = arg.slice('--player='.length).trim();
      continue;
    }

    if (arg.startsWith('--team=')) {
      options.teamSlug = arg.slice('--team='.length).trim();
      continue;
    }

    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
    }
  }

  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/sync-player-contracts.mts --input=<path> --competition=<slug> --season=<slug> [options]

Options:
  --input=<path>        Normalized JSON file produced by fetch-player-contracts-transfermarkt.py or fetch-player-contracts-scraperfc.py
  --competition=<slug>  Internal competition slug (e.g. premier-league)
  --season=<slug>       Internal season slug (e.g. 2025-2026)
  --player=<slug>       Restrict sync to one internal player slug
  --team=<slug>         Restrict/fallback sync to one internal team slug
  --limit=<n>           Limit rows read from the input payload
  --dry-run             Preview matched updates without writing to the database
  --help, -h            Show this help message

Environment:
  DATABASE_URL          PostgreSQL connection string
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 5,
    prepare: false,
  });
}

function resolvePath(inputPath: string) {
  return path.isAbsolute(inputPath) ? inputPath : path.join(process.cwd(), inputPath);
}

async function readPayload(inputPath: string) {
  const resolvedPath = resolvePath(inputPath);
  const raw = await readFile(resolvedPath, 'utf8');
  return JSON.parse(raw) as ContractSyncPayload;
}

function normalizeText(value: string | null | undefined) {
  return value
    ?.normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .toLowerCase() ?? '';
}

function collectUniqueKeys(values: Array<string | null | undefined>) {
  return Array.from(new Set(values.map(normalizeText).filter(Boolean)));
}

function tokenizeNormalized(value: string | null | undefined) {
  return normalizeText(value).split(' ').filter(Boolean);
}

function buildAcronym(value: string | null | undefined) {
  return tokenizeNormalized(value).map((token) => token[0]).join('');
}

function namesMatch(expected: string | null | undefined, actual: string | null | undefined) {
  const expectedTokens = tokenizeNormalized(expected);
  const actualTokens = tokenizeNormalized(actual);
  if (expectedTokens.length === 0 || actualTokens.length === 0) {
    return false;
  }
  if (expectedTokens.join(' ') === actualTokens.join(' ')) {
    return true;
  }
  if (expectedTokens.length === actualTokens.length) {
    return expectedTokens.every((expectedToken, index) => {
      const actualToken = actualTokens[index];
      return expectedToken === actualToken || (expectedToken.length === 1 && actualToken.startsWith(expectedToken));
    });
  }
  return false;
}

function teamNamesMatch(expected: string | null | undefined, actual: string | null | undefined) {
  const expectedNormalized = normalizeText(expected);
  const actualNormalized = normalizeText(actual);
  if (!expectedNormalized || !actualNormalized) {
    return false;
  }
  if (expectedNormalized === actualNormalized) {
    return true;
  }
  return buildAcronym(expected) === actualNormalized || buildAcronym(actual) === expectedNormalized;
}

function parseMoneyValue(value: number | string | null | undefined) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.round(value);
  }

  if (typeof value !== 'string') {
    return undefined;
  }

  const digits = value.replace(/[^0-9-]/g, '');
  if (!digits) {
    return undefined;
  }

  const parsed = Number.parseInt(digits, 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function getMonthIndex(value: string) {
  const months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
  const normalized = value.trim().slice(0, 3).toLowerCase();
  const index = months.indexOf(normalized);
  return index >= 0 ? index : undefined;
}

function getLastDayOfMonth(year: number, monthIndex: number) {
  return new Date(Date.UTC(year, monthIndex + 1, 0)).getUTCDate();
}

function formatIsoDate(year: number, month: number, day: number) {
  return `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
}

function parseDateValue(value: string | null | undefined, boundary: 'start' | 'end') {
  if (!value) {
    return undefined;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }

  const isoMonthMatch = trimmed.match(/^(\d{4})-(\d{2})$/);
  if (isoMonthMatch) {
    const year = Number.parseInt(isoMonthMatch[1], 10);
    const month = Number.parseInt(isoMonthMatch[2], 10);
    if (month >= 1 && month <= 12) {
      const day = boundary === 'start' ? 1 : getLastDayOfMonth(year, month - 1);
      return formatIsoDate(year, month, day);
    }
  }

  const slashMatch = trimmed.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slashMatch) {
    const month = Number.parseInt(slashMatch[1], 10);
    const day = Number.parseInt(slashMatch[2], 10);
    const year = Number.parseInt(slashMatch[3], 10);
    return formatIsoDate(year, month, day);
  }

  const monthDayYearMatch = trimmed.match(/^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$/);
  if (monthDayYearMatch) {
    const monthIndex = getMonthIndex(monthDayYearMatch[1]);
    if (monthIndex !== undefined) {
      return formatIsoDate(
        Number.parseInt(monthDayYearMatch[3], 10),
        monthIndex + 1,
        Number.parseInt(monthDayYearMatch[2], 10)
      );
    }
  }

  const dayMonthYearMatch = trimmed.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (dayMonthYearMatch) {
    const monthIndex = getMonthIndex(dayMonthYearMatch[2]);
    if (monthIndex !== undefined) {
      return formatIsoDate(
        Number.parseInt(dayMonthYearMatch[3], 10),
        monthIndex + 1,
        Number.parseInt(dayMonthYearMatch[1], 10)
      );
    }
  }

  const monthYearMatch = trimmed.match(/^([A-Za-z]+)\s+(\d{4})$/);
  if (monthYearMatch) {
    const monthIndex = getMonthIndex(monthYearMatch[1]);
    if (monthIndex !== undefined) {
      const year = Number.parseInt(monthYearMatch[2], 10);
      const day = boundary === 'start' ? 1 : getLastDayOfMonth(year, monthIndex);
      return formatIsoDate(year, monthIndex + 1, day);
    }
  }

  const parsed = new Date(trimmed);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }

  return undefined;
}

async function ensureDataSource(sql: Sql, provider: string) {
  const slug = provider === 'transfermarkt' ? 'transfermarkt' : `${provider}_scraperfc`;
  const sourceName = provider === 'capology' ? 'Capology via ScraperFC' : provider === 'transfermarkt' ? 'Transfermarkt Player Profiles' : provider;
  const baseUrl = provider === 'capology' ? 'https://www.capology.com' : provider === 'transfermarkt' ? 'https://www.transfermarkt.com' : null;
  const rows = await sql<SourceRow[]>`
    INSERT INTO data_sources (slug, name, base_url, source_kind, upstream_ref, priority)
    VALUES (${slug}, ${sourceName}, ${baseUrl}, 'scraper', 'scraperfc', 3)
    ON CONFLICT (slug)
    DO UPDATE SET
      name = EXCLUDED.name,
      base_url = EXCLUDED.base_url,
      source_kind = EXCLUDED.source_kind,
      upstream_ref = EXCLUDED.upstream_ref,
      priority = EXCLUDED.priority
    RETURNING id
  `;

  return rows[0].id;
}

async function createSyncRun(sql: Sql, sourceId: number, metadata: Record<string, unknown>) {
  const rows = await sql<SyncRunRow[]>`
    INSERT INTO source_sync_runs (source_id, upstream_ref, status, metadata)
    VALUES (${sourceId}, 'scraperfc', 'running', ${JSON.stringify(metadata)}::jsonb)
    RETURNING id
  `;

  return rows[0].id;
}

async function finishSyncRun(sql: Sql, syncRunId: number, status: 'completed' | 'failed', summary: Record<string, unknown>) {
  await sql`
    UPDATE source_sync_runs
    SET
      status = ${status},
      fetched_files = 1,
      changed_files = ${Number(summary.updated ?? 0)},
      completed_at = NOW(),
      metadata = COALESCE(metadata, '{}'::jsonb) || ${JSON.stringify(summary)}::jsonb
    WHERE id = ${syncRunId}
  `;
}

async function insertRawPayload(
  sql: Sql,
  params: {
    payload: unknown;
    playerSlug: string | null;
    provider: string;
    seasonSlug: string;
    sourceId: number;
    syncRunId: number;
  }
) {
  await sql`
    INSERT INTO raw_payloads (
      source_id,
      sync_run_id,
      endpoint,
      entity_type,
      external_id,
      season_context,
      http_status,
      payload
    )
    VALUES (
      ${params.sourceId},
      ${params.syncRunId},
      ${`${params.provider}:player_profile`},
      'player',
      ${params.playerSlug},
      ${params.seasonSlug},
      200,
      ${JSON.stringify(params.payload)}::jsonb
    )
  `;
}

async function loadTargets(sql: Sql, competitionSlug: string, seasonSlug: string, playerSlug?: string, teamSlug?: string) {
  const rows = playerSlug
    ? await sql<SyncTargetRow[]>`
        SELECT
          pc.id AS contract_id,
          pc.competition_season_id,
          p.slug AS player_slug,
          pt.known_as,
          CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
          t.slug AS team_slug,
          tt.name AS team_name,
          tt.short_name
        FROM player_contracts pc
        JOIN players p ON p.id = pc.player_id
        JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
        JOIN teams t ON t.id = pc.team_id
        JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
        JOIN competition_seasons cs ON cs.id = pc.competition_season_id
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE c.slug = ${competitionSlug}
          AND s.slug = ${seasonSlug}
          AND pc.left_date IS NULL
          AND p.slug = ${playerSlug}
          AND (${teamSlug ?? null}::text IS NULL OR t.slug = ${teamSlug ?? null})
      `
    : await sql<SyncTargetRow[]>`
        SELECT
          pc.id AS contract_id,
          pc.competition_season_id,
          p.slug AS player_slug,
          pt.known_as,
          CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
          t.slug AS team_slug,
          tt.name AS team_name,
          tt.short_name
        FROM player_contracts pc
        JOIN players p ON p.id = pc.player_id
        JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
        JOIN teams t ON t.id = pc.team_id
        JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
        JOIN competition_seasons cs ON cs.id = pc.competition_season_id
        JOIN competitions c ON c.id = cs.competition_id
        JOIN seasons s ON s.id = cs.season_id
        WHERE c.slug = ${competitionSlug}
          AND s.slug = ${seasonSlug}
          AND pc.left_date IS NULL
          AND (${teamSlug ?? null}::text IS NULL OR t.slug = ${teamSlug ?? null})
      `;

  const fallbackRows = rows.length > 0 || !teamSlug
    ? rows
    : await sql<SyncTargetRow[]>`
        WITH target_season AS (
          SELECT start_date
          FROM seasons
          WHERE slug = ${seasonSlug}
          LIMIT 1
        ), latest_team_contracts AS (
          SELECT DISTINCT ON (pc.player_id)
            pc.id AS contract_id,
            pc.competition_season_id,
            p.slug AS player_slug,
            pt.known_as,
            CONCAT_WS(' ', pt.first_name, pt.last_name) AS full_name,
            t.slug AS team_slug,
            tt.name AS team_name,
            tt.short_name,
            COALESCE(pc.left_date, pc.contract_end_date, pc.joined_date, s.end_date, s.start_date) AS recency_date
          FROM player_contracts pc
          CROSS JOIN target_season target
          JOIN players p ON p.id = pc.player_id
          JOIN player_translations pt ON pt.player_id = p.id AND pt.locale = 'en'
          JOIN teams t ON t.id = pc.team_id
          JOIN team_translations tt ON tt.team_id = t.id AND tt.locale = 'en'
          JOIN competition_seasons cs ON cs.id = pc.competition_season_id
          JOIN competitions c ON c.id = cs.competition_id
          JOIN seasons s ON s.id = cs.season_id
          WHERE pc.left_date IS NULL
            AND t.slug = ${teamSlug}
            AND (${playerSlug ?? null}::text IS NULL OR p.slug = ${playerSlug ?? null})
            AND COALESCE(pc.left_date, pc.contract_end_date, pc.joined_date, s.end_date, s.start_date) IS NOT NULL
            AND COALESCE(pc.left_date, pc.contract_end_date, pc.joined_date, s.end_date, s.start_date) >= target.start_date - INTERVAL '18 months'
          ORDER BY pc.player_id, recency_date DESC NULLS LAST, pc.joined_date DESC NULLS LAST, pc.id DESC
        )
        SELECT contract_id, competition_season_id, player_slug, known_as, full_name, team_slug, team_name, short_name
        FROM latest_team_contracts
      `;

  return fallbackRows.map<SyncTarget>((row) => ({
    competitionSeasonId: row.competition_season_id,
    contractId: row.contract_id,
    playerSlug: row.player_slug,
    playerKeys: collectUniqueKeys([row.known_as, row.full_name]),
    teamKeys: collectUniqueKeys([row.team_name, row.short_name, row.team_slug]),
  }));
}

function normalizeCountryKey(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

async function loadCountryRows(sql: Sql) {
  return sql<CountryLookupRow[]>`
    SELECT
      c.id,
      c.code_alpha2,
      c.code_alpha3,
      ct.name AS translation_name
    FROM countries c
    LEFT JOIN country_translations ct ON ct.country_id = c.id AND ct.locale = 'en'
  `;
}

function buildCountryResolver(rows: CountryLookupRow[], countryCodeResolver: CountryCodeResolver) {
  const countryIdByKey = new Map<string, number>();
  const countryIdByCode = new Map<string, number>();
  const regionNames = new Intl.DisplayNames(['en'], { type: 'region' });

  for (const row of rows) {
    countryIdByCode.set(countryCodeResolver.resolve(row.code_alpha3) ?? row.code_alpha3, row.id);

    const displayName = row.code_alpha2 ? regionNames.of(row.code_alpha2.toUpperCase()) : undefined;
    for (const candidate of [row.code_alpha2, row.code_alpha3, row.translation_name, displayName]) {
      if (!candidate) {
        continue;
      }
      countryIdByKey.set(normalizeCountryKey(candidate), row.id);
    }
  }

  for (const [alias, code] of Object.entries(COUNTRY_NAME_ALIASES)) {
    const countryId = countryIdByCode.get(countryCodeResolver.resolve(code) ?? code);
    if (countryId) {
      countryIdByKey.set(alias, countryId);
    }
  }

  return {
    resolve(values: string[] | null | undefined) {
      for (const value of values ?? []) {
        const normalized = normalizeCountryKey(value);
        if (!normalized) {
          continue;
        }

        const directMatch = countryIdByKey.get(normalized);
        if (directMatch) {
          return directMatch;
        }

        const aliasCode = COUNTRY_NAME_ALIASES[normalized];
        if (!aliasCode) {
          continue;
        }

        const aliasMatch = countryIdByCode.get(countryCodeResolver.resolve(aliasCode) ?? aliasCode);
        if (aliasMatch) {
          return aliasMatch;
        }
      }

      return undefined;
    },
  };
}

function matchTarget(row: ContractSyncRow, targets: SyncTarget[]) {
  if (row.playerSlug) {
    const slugMatch = targets.find((target) => target.playerSlug === row.playerSlug);
    if (slugMatch) {
      return slugMatch;
    }
  }

  const playerKey = normalizeText(row.playerName);
  const teamKey = normalizeText(row.teamName);
  if (!playerKey) {
    return null;
  }

  const exact = targets.find((target) => {
    const playerMatches = target.playerKeys.some((candidate) => namesMatch(candidate, row.playerName));
    if (!playerMatches) {
      return false;
    }
    if (!teamKey) {
      return true;
    }
    return target.teamKeys.some((candidate) => teamNamesMatch(candidate, row.teamName));
  });
  if (exact) {
    return exact;
  }

  const playerMatches = targets.filter((target) => target.playerKeys.some((candidate) => namesMatch(candidate, row.playerName)));
  return playerMatches.length === 1 ? playerMatches[0] : null;
}

function isTransfermarktProfileSync(provider: string) {
  return provider === 'transfermarkt';
}

function parsePosition(value: string | null | undefined): 'GK' | 'DEF' | 'MID' | 'FWD' | undefined {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) {
    return undefined;
  }

  if (normalized.includes('goalkeeper')) {
    return 'GK';
  }
  if (
    normalized.includes('back')
    || normalized.includes('defender')
    || normalized.includes('sweeper')
    || normalized.includes('full-back')
    || normalized.includes('centre-back')
    || normalized.includes('center-back')
    || normalized.includes('full back')
  ) {
    return 'DEF';
  }
  if (
    normalized.includes('winger')
    || normalized.includes('forward')
    || normalized.includes('striker')
    || normalized.includes('second striker')
    || normalized.includes('centre-forward')
    || normalized.includes('center-forward')
  ) {
    return 'FWD';
  }
  if (
    normalized.includes('midfield')
    || normalized.includes('midfielder')
    || normalized.includes('attacking mid')
    || normalized.includes('defensive mid')
    || normalized.includes('central mid')
  ) {
    return 'MID';
  }

  return undefined;
}

async function updateContract(
  sql: Sql,
  params: {
    contractId: number;
    provider: string;
    row: ContractSyncRow;
  }
) {
  const contractStartDate = parseDateValue(params.row.contractStartDate ?? undefined, 'start');
  const contractEndDate = parseDateValue(params.row.contractEndDate ?? undefined, 'end');
  const annualSalary = parseMoneyValue(params.row.annualSalary);
  const marketValue = parseMoneyValue(params.row.marketValue);
  const weeklyWage = parseMoneyValue(params.row.weeklyWage);
  const currencyCode = params.row.currencyCode?.trim() || undefined;
  const sourceUrl = params.row.sourceUrl?.trim() || undefined;
  const hasSalaryData = annualSalary !== undefined || weeklyWage !== undefined;
  const hasMarketValueData = marketValue !== undefined;

  if (hasSalaryData) {
    await sql`
      UPDATE player_contracts
      SET
        contract_start_date = COALESCE(${contractStartDate ?? null}, contract_start_date),
        contract_end_date = COALESCE(${contractEndDate ?? null}, contract_end_date),
        annual_salary_eur = COALESCE(${annualSalary ?? null}, annual_salary_eur),
        weekly_wage_eur = COALESCE(${weeklyWage ?? null}, weekly_wage_eur),
        salary_currency = COALESCE(${currencyCode ?? null}, salary_currency),
        salary_source = ${params.provider},
        salary_source_url = COALESCE(${sourceUrl ?? null}, salary_source_url),
        salary_is_estimated = COALESCE(${params.row.isEstimated ?? null}, salary_is_estimated),
        salary_updated_at = NOW(),
        market_value_eur = COALESCE(${marketValue ?? null}, market_value_eur),
        market_value_source = CASE WHEN ${marketValue ?? null} IS NOT NULL THEN ${params.provider} ELSE market_value_source END,
        market_value_source_url = COALESCE(${sourceUrl ?? null}, market_value_source_url),
        market_value_updated_at = CASE WHEN ${marketValue ?? null} IS NOT NULL THEN NOW() ELSE market_value_updated_at END,
        updated_at = NOW()
      WHERE id = ${params.contractId}
    `;
    return;
  }

  if (hasMarketValueData) {
    await sql`
      UPDATE player_contracts
      SET
        contract_start_date = COALESCE(${contractStartDate ?? null}, contract_start_date),
        contract_end_date = COALESCE(${contractEndDate ?? null}, contract_end_date),
        market_value_eur = COALESCE(${marketValue ?? null}, market_value_eur),
        market_value_source = ${params.provider},
        market_value_source_url = COALESCE(${sourceUrl ?? null}, market_value_source_url),
        market_value_updated_at = NOW(),
        updated_at = NOW()
      WHERE id = ${params.contractId}
    `;
    return;
  }

  await sql`
    UPDATE player_contracts
    SET
      contract_start_date = COALESCE(${contractStartDate ?? null}, contract_start_date),
      contract_end_date = COALESCE(${contractEndDate ?? null}, contract_end_date),
      updated_at = NOW()
    WHERE id = ${params.contractId}
  `;
}

async function updatePlayerProfile(
  sql: Sql,
  params: {
    contractId: number;
    countryId?: number;
    provider: string;
    row: ContractSyncRow;
  }
) {
  const dateOfBirth = parseDateValue(params.row.dateOfBirth ?? undefined, 'start');
  const parsedHeight = typeof params.row.heightCm === 'number'
    ? Math.round(params.row.heightCm)
    : typeof params.row.heightCm === 'string'
      ? Number.parseInt(params.row.heightCm.replace(/[^0-9-]/g, ''), 10)
      : undefined;
  const heightCm = Number.isFinite(parsedHeight) ? parsedHeight : undefined;
  const normalizedPreferredFoot = params.row.preferredFoot?.trim().toLowerCase();
  const preferredFoot = normalizedPreferredFoot?.startsWith('left')
    ? 'Left'
    : normalizedPreferredFoot?.startsWith('right')
      ? 'Right'
      : normalizedPreferredFoot?.startsWith('both') || normalizedPreferredFoot?.startsWith('either')
        ? 'Both'
        : undefined;
  const position = parsePosition(params.row.position);
  const photoUrl = params.row.photoUrl?.trim() || undefined;
  const externalId = params.row.sourceUrl?.match(/\/spieler\/(\d+)/)?.[1] ?? null;

  if (!dateOfBirth && heightCm === undefined && !preferredFoot && !position && !photoUrl && !params.countryId) {
    return;
  }

  await sql`
    UPDATE players p
    SET
      date_of_birth = COALESCE(p.date_of_birth, ${dateOfBirth ?? null}::date),
      country_id = COALESCE(p.country_id, ${params.countryId ?? null}::bigint),
      height_cm = COALESCE(p.height_cm, ${heightCm ?? null}::integer),
      position = COALESCE(p.position, ${position ?? null}::position_type),
      photo_url = COALESCE(p.photo_url, ${photoUrl ?? null}),
      preferred_foot = COALESCE(p.preferred_foot, ${preferredFoot ?? null}::preferred_foot),
      updated_at = CASE
        WHEN (${dateOfBirth ?? null}::date IS NOT NULL AND p.date_of_birth IS NULL)
          OR (${params.countryId ?? null}::bigint IS NOT NULL AND p.country_id IS NULL)
          OR (${heightCm ?? null}::integer IS NOT NULL AND p.height_cm IS NULL)
          OR (${position ?? null}::position_type IS NOT NULL AND p.position IS NULL)
          OR (${photoUrl ?? null}::text IS NOT NULL AND p.photo_url IS NULL)
          OR (${preferredFoot ?? null}::preferred_foot IS NOT NULL AND p.preferred_foot IS NULL)
        THEN NOW()
        ELSE p.updated_at
      END
    FROM player_contracts pc
    WHERE pc.id = ${params.contractId}
      AND pc.player_id = p.id
  `;

  if (photoUrl) {
    await sql`
      INSERT INTO player_photo_sources (
        player_id,
        data_source_id,
        external_id,
        source_url,
        mirrored_url,
        status,
        matched_by,
        last_checked_at,
        last_synced_at,
        failure_count,
        updated_at
      )
      VALUES (
        (SELECT player_id FROM player_contracts WHERE id = ${params.contractId}),
        (SELECT id FROM data_sources WHERE slug = ${params.provider}),
        ${externalId},
        ${photoUrl},
        NULL,
        'active',
        'transfermarkt_profile',
        NOW(),
        NOW(),
        0,
        NOW()
      )
      ON CONFLICT (player_id, data_source_id)
      DO UPDATE SET
        external_id = COALESCE(EXCLUDED.external_id, player_photo_sources.external_id),
        source_url = EXCLUDED.source_url,
        status = 'active',
        matched_by = EXCLUDED.matched_by,
        last_checked_at = EXCLUDED.last_checked_at,
        last_synced_at = EXCLUDED.last_synced_at,
        failure_count = 0,
        last_error = NULL,
        updated_at = NOW()
    `;
  }

  if (externalId) {
    await sql`
      INSERT INTO source_entity_mapping (entity_type, entity_id, source_id, external_id, metadata)
      VALUES (
        'player',
        (SELECT player_id FROM player_contracts WHERE id = ${params.contractId}),
        (SELECT id FROM data_sources WHERE slug = ${params.provider}),
        ${externalId},
        ${JSON.stringify({ sourceUrl: params.row.sourceUrl ?? null })}::jsonb
      )
      ON CONFLICT (entity_type, source_id, external_id)
      DO UPDATE SET
        metadata = COALESCE(source_entity_mapping.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = NOW()
    `;
  }
}

function printSummary(summary: Record<string, unknown>) {
  console.log(JSON.stringify(summary, null, 2));
}

async function main() {
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  if (!options.inputPath || !options.competitionSlug || !options.seasonSlug) {
    throw new Error('--input, --competition, and --season are required');
  }

  const payload = await readPayload(options.inputPath);
  const provider = payload.provider.trim().toLowerCase();
  const rows = options.limit ? payload.rows.slice(0, options.limit) : payload.rows;
  const sql = getSql();

  let syncRunId: number | null = null;
  try {
    const countryCodeResolver = await loadCountryCodeResolver(sql);
    const targets = await loadTargets(sql, options.competitionSlug, options.seasonSlug, options.playerSlug, options.teamSlug);
    const countryResolver = buildCountryResolver(await loadCountryRows(sql), countryCodeResolver);
    const sourceId = options.dryRun ? null : await ensureDataSource(sql, provider);

    if (!options.dryRun && sourceId !== null) {
      syncRunId = await createSyncRun(sql, sourceId, {
        competitionSlug: options.competitionSlug,
        fetchedAt: payload.fetchedAt ?? null,
        inputPath: resolvePath(options.inputPath),
        provider,
        requestedCompetition: payload.competition ?? null,
        requestedSeason: payload.season ?? null,
        seasonSlug: options.seasonSlug,
        sourceUrl: payload.sourceUrl ?? null,
      });
    }

    let matched = 0;
    let unmatched = 0;
    let updated = 0;
    let skipped = 0;

    for (const row of rows) {
      const target = matchTarget(row, targets);
      if (!target) {
        unmatched += 1;
        continue;
      }

      matched += 1;
      const hasAnyValue = Boolean(
        parseDateValue(row.dateOfBirth ?? undefined, 'start')
        ||
        parseDateValue(row.contractStartDate ?? undefined, 'start')
        || parseDateValue(row.contractEndDate ?? undefined, 'end')
        || parseMoneyValue(row.annualSalary)
        || parseMoneyValue(row.marketValue)
        || parseMoneyValue(row.weeklyWage)
        || (typeof row.heightCm === 'number' && Number.isFinite(row.heightCm))
        || (typeof row.heightCm === 'string' && row.heightCm.trim())
        || row.photoUrl?.trim()
        || row.position?.trim()
        || row.preferredFoot?.trim()
        || row.nationalities?.length
      );

      if (!hasAnyValue) {
        skipped += 1;
        continue;
      }

      if (!options.dryRun) {
        await updatePlayerProfile(sql, {
          contractId: target.contractId,
          countryId: countryResolver.resolve(row.nationalities),
          provider,
          row,
        });

        await updateContract(sql, {
          contractId: target.contractId,
          provider,
          row: {
            ...row,
            currencyCode: row.currencyCode ?? payload.currencyCode ?? undefined,
            sourceUrl: row.sourceUrl ?? payload.sourceUrl ?? undefined,
          },
        });

        if (sourceId !== null && syncRunId !== null) {
          await insertRawPayload(sql, {
            payload: row.raw ?? row,
            playerSlug: target.playerSlug,
            provider,
            seasonSlug: options.seasonSlug,
            sourceId,
            syncRunId,
          });
        }
      }

      updated += 1;
    }

    const summary: SyncSummary = {
      matched,
      provider,
      skipped,
      unmatched,
      updated,
    };
    const summaryPayload: SummaryPayload = {
      matched: summary.matched,
      provider: summary.provider,
      skipped: summary.skipped,
      unmatched: summary.unmatched,
      updated: summary.updated,
    };

    if (!options.dryRun && syncRunId !== null) {
      await finishSyncRun(sql, syncRunId, 'completed', summaryPayload);
    }

    printSummary(summaryPayload);
  } catch (error) {
    if (syncRunId !== null) {
      await finishSyncRun(sql, syncRunId, 'failed', {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    throw error;
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
