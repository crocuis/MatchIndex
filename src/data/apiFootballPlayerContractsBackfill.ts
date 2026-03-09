import postgres, { type Sql } from 'postgres';
import { parseApiFootballCompetitionTargets, type ApiFootballCompetitionTarget } from './apiFootball.ts';

interface SourceRow { id: number; }
interface MappingRow { entity_id: number; external_id: string; }
interface RawPayloadRow { payload: unknown; }
interface TeamRow { id: number; slug: string; name: string | null; }
interface CompetitionSeasonRow { id: number; competition_slug: string; season_start_year: number; }

interface ApiFootballPlayerPayload {
  id?: number | string;
}

interface ApiFootballStatisticPayload {
  team?: { name?: string; id?: number | string };
  league?: { id?: number | string; season?: number | string };
  games?: { number?: number | null };
}

interface ApiFootballPlayerEntry {
  player?: ApiFootballPlayerPayload;
  statistics?: ApiFootballStatisticPayload[];
}

interface ContractDraft {
  playerId: number;
  teamId: number;
  competitionSeasonId: number;
  shirtNumber: number | null;
}

export interface BackfillApiFootballPlayerContractsOptions {
  dryRun?: boolean;
  competitionCodes?: string[];
  seasons?: number[];
}

export interface BackfillApiFootballPlayerContractsSummary {
  dryRun: boolean;
  rawPayloadsRead: number;
  playerMappingsFound: number;
  teamMatchesFound: number;
  contractRowsPlanned: number;
  contractRowsWritten: number;
  unresolvedTeamNames: string[];
}

function getDb() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) throw new Error('DATABASE_URL is not set');
  return postgres(connectionString, { max: 1, idle_timeout: 20, prepare: false });
}

function normalizeSeasons(input?: number[]) {
  if (input && input.length > 0) return [...new Set(input)].sort((a, b) => a - b);
  return [new Date().getUTCFullYear()];
}

function normalizeName(value: string) {
  return value.normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-zA-Z0-9]+/g, ' ').trim().toLowerCase();
}

function resolveCanonicalTeamSlug(value: string) {
  const normalized = normalizeName(value);
  const exact = new Map<string, string>([
    ['wolves', 'wolverhampton-wanderers-england'],
    ['tottenham', 'tottenham-hotspur-england'],
    ['west ham', 'west-ham-united-england'],
    ['sheffield utd', 'sheffield-united-england'],
    ['alaves', 'deportivo-alaves-spain'],
    ['sevilla', 'sevilla-spain'],
    ['granada cf', 'granada-spain'],
    ['luton', 'luton-town-england'],
  ]);

  return exact.get(normalized) ?? null;
}

function expandTeamAliases(value: string) {
  const normalized = normalizeName(value);
  const aliases = new Set<string>([normalized]);
  aliases.add(normalized.replace(/\b(cf|fc|afc|ac)\b/g, '').replace(/\s+/g, ' ').trim());

  if (normalized === 'wolves') {
    aliases.add('wolverhampton wanderers');
    aliases.add('wolverhampton wanderers england');
  }

  if (normalized === 'tottenham') {
    aliases.add('tottenham hotspur');
    aliases.add('tottenham hotspur england');
  }

  if (normalized === 'sevilla') {
    aliases.add('sevilla fc');
  }

  if (normalized === 'granada cf') {
    aliases.add('granada');
    aliases.add('granada spain');
  }

  if (normalized === 'west ham') {
    aliases.add('west ham united');
    aliases.add('west ham united england');
  }

  if (normalized === 'alaves') {
    aliases.add('deportivo alaves');
    aliases.add('deportivo alaves spain');
  }

  if (normalized === 'sheffield utd') {
    aliases.add('sheffield united');
    aliases.add('sheffield united england');
  }

  if (normalized === 'luton') {
    aliases.add('luton town');
    aliases.add('luton town england');
  }

  return Array.from(aliases).filter(Boolean);
}

async function ensureBootstrapTeam(sql: Sql, slug: string, countryCode: string, name: string) {
  await sql`
    INSERT INTO teams (slug, country_id, gender, is_national, is_active, updated_at)
    VALUES (
      ${slug},
      (SELECT id FROM countries WHERE code_alpha3 = ${countryCode}),
      'male',
      FALSE,
      TRUE,
      NOW()
    )
    ON CONFLICT (slug)
    DO UPDATE SET updated_at = NOW(), is_active = TRUE
  `;

  await sql`
    INSERT INTO team_translations (team_id, locale, name, short_name)
    VALUES ((SELECT id FROM teams WHERE slug = ${slug}), 'en', ${name}, ${name})
    ON CONFLICT (team_id, locale)
    DO UPDATE SET name = EXCLUDED.name, short_name = EXCLUDED.short_name
  `;
}

async function ensureBootstrapTeams(sql: Sql) {
  await ensureBootstrapTeam(sql, 'luton-town-england', 'ENG', 'Luton Town');
  await ensureBootstrapTeam(sql, 'sheffield-united-england', 'ENG', 'Sheffield United');
}

function getPayloadEntries(payload: unknown) {
  const parsed = typeof payload === 'string' ? JSON.parse(payload) : payload;
  if (!parsed || typeof parsed !== 'object') return [] as ApiFootballPlayerEntry[];
  const response = (parsed as { response?: unknown[] }).response;
  return Array.isArray(response) ? response as ApiFootballPlayerEntry[] : [];
}

async function ensureSource(sql: Sql) {
  const rows = await sql<SourceRow[]>`select id from data_sources where slug = 'api_football' limit 1`;
  if (!rows[0]) throw new Error('API-Football data source is not initialized');
  return rows[0].id;
}

async function loadPlayerMappings(sql: Sql, sourceId: number) {
  const rows = await sql<MappingRow[]>`
    select entity_id, external_id
    from source_entity_mapping
    where entity_type='player' and source_id=${sourceId}
  `;
  return new Map(rows.map((row) => [row.external_id, Number(row.entity_id)]));
}

async function loadRawPayloads(sql: Sql, sourceId: number, seasons: number[]) {
  return sql<RawPayloadRow[]>`
    select distinct on (endpoint) payload
    from raw_payloads
    where source_id=${sourceId} and entity_type='player' and season_context = any(${seasons.map(String)})
    order by endpoint, fetched_at desc
  `;
}

async function loadCompetitionSeasons(sql: Sql, targets: ApiFootballCompetitionTarget[], seasons: number[]) {
  const rows = await sql<CompetitionSeasonRow[]>`
    select cs.id, c.slug as competition_slug, extract(year from s.start_date)::int as season_start_year
    from competition_seasons cs
    join competitions c on c.id = cs.competition_id
    join seasons s on s.id = cs.season_id
    where c.slug = any(${targets.map((target) => target.competitionSlug)})
      and extract(year from s.start_date)::int = any(${seasons})
  `;
  return new Map(rows.map((row) => [`${row.competition_slug}:${row.season_start_year}`, Number(row.id)]));
}

async function loadTeams(sql: Sql) {
  return sql<TeamRow[]>`
    select t.id, t.slug, tt.name
    from teams t
    left join team_translations tt on tt.team_id=t.id and tt.locale='en'
  `;
}

function buildTeamIndex(teams: TeamRow[]) {
  const index = new Map<string, TeamRow[]>();
  const bySlug = new Map<string, TeamRow>();
  for (const team of teams) {
    bySlug.set(team.slug, team);
    const variants = [team.name, team.slug.replace(/-/g, ' ')]
      .filter(Boolean)
      .flatMap((value) => expandTeamAliases(String(value)));
    for (const variant of new Set(variants)) {
      const current = index.get(variant) ?? [];
      current.push(team);
      index.set(variant, current);
    }
  }
  return { index, bySlug };
}

async function upsertContract(sql: Sql, draft: ContractDraft) {
  await sql`
    insert into player_contracts (
      player_id, team_id, competition_season_id, shirt_number, is_on_loan, left_date, updated_at
    ) values (
      ${draft.playerId}, ${draft.teamId}, ${draft.competitionSeasonId}, ${draft.shirtNumber}, false, null, now()
    )
    on conflict (player_id, competition_season_id)
    do update set
      team_id = excluded.team_id,
      shirt_number = excluded.shirt_number,
      is_on_loan = excluded.is_on_loan,
      left_date = excluded.left_date,
      updated_at = now()
  `;

  await sql`
    insert into team_seasons (team_id, competition_season_id, updated_at)
    values (${draft.teamId}, ${draft.competitionSeasonId}, now())
    on conflict (team_id, competition_season_id)
    do update set updated_at = now()
  `;
}

export async function backfillApiFootballPlayerContracts(
  options: BackfillApiFootballPlayerContractsOptions = {},
): Promise<BackfillApiFootballPlayerContractsSummary> {
  const seasons = normalizeSeasons(options.seasons);
  const targets = parseApiFootballCompetitionTargets(options.competitionCodes);
  const targetByLeagueId = new Map(targets.map((target) => [String(target.leagueId), target]));
  const sql = getDb();

  try {
    const sourceId = await ensureSource(sql);
    await ensureBootstrapTeams(sql);
    const playerMappings = await loadPlayerMappings(sql, sourceId);
    const rawPayloads = await loadRawPayloads(sql, sourceId, seasons);
    const competitionSeasonIds = await loadCompetitionSeasons(sql, targets, seasons);
    const { index: teamIndex, bySlug: teamBySlug } = buildTeamIndex(await loadTeams(sql));
    const unresolvedTeamNames = new Set<string>();
    const drafts = new Map<string, ContractDraft>();
    let teamMatchesFound = 0;

    for (const rawPayload of rawPayloads) {
      for (const entry of getPayloadEntries(rawPayload.payload)) {
        const externalPlayerId = entry.player?.id ? String(entry.player.id) : null;
        if (!externalPlayerId) continue;
        const playerId = playerMappings.get(externalPlayerId);
        if (!playerId) continue;

        for (const statistic of entry.statistics ?? []) {
          const leagueId = statistic.league?.id ? String(statistic.league.id) : null;
          const season = statistic.league?.season ? Number(statistic.league.season) : Number.NaN;
          const teamName = statistic.team?.name?.trim();
          if (!leagueId || !Number.isFinite(season) || !teamName) continue;

          const target = targetByLeagueId.get(leagueId);
          if (!target) continue;
          const competitionSeasonId = competitionSeasonIds.get(`${target.competitionSlug}:${season}`);
          if (!competitionSeasonId) continue;

          const explicitTeamSlug = resolveCanonicalTeamSlug(teamName);
          const teams = explicitTeamSlug && teamBySlug.has(explicitTeamSlug)
            ? [teamBySlug.get(explicitTeamSlug)].filter((value): value is TeamRow => Boolean(value))
            : Array.from(new Map(
              expandTeamAliases(teamName)
                .flatMap((alias) => teamIndex.get(alias) ?? [])
                .map((team) => [team.id, team])
            ).values());
          if (teams.length !== 1) {
            unresolvedTeamNames.add(teamName);
            continue;
          }

          teamMatchesFound += 1;
          drafts.set(`${playerId}:${competitionSeasonId}`, {
            playerId,
            teamId: Number(teams[0].id),
            competitionSeasonId,
            shirtNumber: statistic.games?.number ?? null,
          });
        }
      }
    }

    if (options.dryRun ?? true) {
      return {
        dryRun: true,
        rawPayloadsRead: rawPayloads.length,
        playerMappingsFound: playerMappings.size,
        teamMatchesFound,
        contractRowsPlanned: drafts.size,
        contractRowsWritten: 0,
        unresolvedTeamNames: Array.from(unresolvedTeamNames).sort(),
      };
    }

    for (const draft of drafts.values()) {
      await upsertContract(sql, draft);
    }

    return {
      dryRun: false,
      rawPayloadsRead: rawPayloads.length,
      playerMappingsFound: playerMappings.size,
      teamMatchesFound,
      contractRowsPlanned: drafts.size,
      contractRowsWritten: drafts.size,
      unresolvedTeamNames: Array.from(unresolvedTeamNames).sort(),
    };
  } finally {
    await sql.end({ timeout: 1 }).catch(() => undefined);
  }
}
