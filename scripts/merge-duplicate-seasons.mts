import postgres, { type Sql } from 'postgres';
import { loadProjectEnv } from './load-project-env.mts';

interface CliOptions {
  dryRun: boolean;
  help: boolean;
  onlySlug?: string;
  skipRefresh: boolean;
}

interface SeasonRow {
  id: number;
  slug: string;
  start_date: string;
  end_date: string;
  is_current: boolean;
}

interface CompetitionSeasonRow {
  id: number;
  competition_id: number;
  current_matchday: number | null;
  total_matchdays: number | null;
  source_match_updated_at: string | null;
  source_match_available_at: string | null;
  source_match_updated_360_at: string | null;
  source_match_available_360_at: string | null;
  source_metadata: Record<string, unknown> | null;
  status: string;
  winner_team_id: number | null;
}

interface SeasonMergePlan {
  alias: SeasonRow;
  canonical: SeasonRow | null;
  canonicalSlug: string;
}

interface MergeSummary {
  aliasSlug: string;
  canonicalSlug: string;
  canonicalSeasonId: number | null;
  aliasSeasonId: number;
  movedCompetitionSeasons: number;
  mergedCompetitionSeasons: number;
  updatedSeasonContexts: number;
  deletedAliasSeason: boolean;
}

function parseArgs(argv: string[]): CliOptions {
  const onlyArg = argv.find((arg) => arg.startsWith('--only='));

  return {
    dryRun: !argv.includes('--write'),
    help: argv.includes('--help') || argv.includes('-h'),
    onlySlug: onlyArg?.slice('--only='.length).trim() || undefined,
    skipRefresh: argv.includes('--skip-refresh'),
  };
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/merge-duplicate-seasons.mts [options]

Options:
  --write              Apply changes (default: dry run)
  --only=<slug>        Merge only one alias season slug
  --skip-refresh       Skip materialized view refresh
  --help, -h           Show this help message

Examples:
  node --experimental-strip-types scripts/merge-duplicate-seasons.mts
  node --experimental-strip-types scripts/merge-duplicate-seasons.mts --write
  node --experimental-strip-types scripts/merge-duplicate-seasons.mts --only=2023-2024 --write
`);
}

function getSql() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) {
    throw new Error('DATABASE_URL is not set');
  }

  return postgres(connectionString, {
    max: 1,
    idle_timeout: 20,
    prepare: false,
  });
}

function normalizeSeasonSlug(slug: string) {
  const shortRangeMatch = slug.match(/^(\d{4})-(\d{2})$/);
  if (shortRangeMatch) {
    const [, startYear, endYear] = shortRangeMatch;
    return `${startYear}/${endYear}`;
  }

  const fullRangeMatch = slug.match(/^(\d{4})-(\d{4})$/);
  if (!fullRangeMatch) {
    return slug;
  }

  const [, startYear, endYear] = fullRangeMatch;
  return `${startYear}/${endYear.slice(-2)}`;
}

function pickEarlierDate(left: string, right: string) {
  return left <= right ? left : right;
}

function pickLaterDate(left: string, right: string) {
  return left >= right ? left : right;
}

function isSeasonCurrentByDate(startDate: string, endDate: string, now: Date = new Date()) {
  const today = now.toISOString().slice(0, 10);
  return startDate <= today && today <= endDate;
}

function mergeStatus(left: string, right: string) {
  const rank: Record<string, number> = {
    active: 1,
    scheduled: 1,
    timed: 1,
    in_progress: 2,
    live: 2,
    completed: 3,
    finished: 3,
  };

  return (rank[right] ?? 0) > (rank[left] ?? 0) ? right : left;
}

function mergeMetadata(left: Record<string, unknown> | null, right: Record<string, unknown> | null) {
  return {
    ...(left ?? {}),
    ...(right ?? {}),
  };
}

async function loadSeasonRows(sql: Sql) {
  return sql<SeasonRow[]>`
    SELECT id, slug, start_date::text, end_date::text, is_current
    FROM seasons
    ORDER BY slug ASC, id ASC
  `;
}

function buildMergePlans(seasons: SeasonRow[], onlySlug?: string) {
  const bySlug = new Map(seasons.map((season) => [season.slug, season]));

  return seasons
    .filter((season) => normalizeSeasonSlug(season.slug) !== season.slug)
    .filter((season) => !onlySlug || season.slug === onlySlug)
    .map((alias) => {
      const canonicalSlug = normalizeSeasonSlug(alias.slug);
      return {
        alias,
        canonical: bySlug.get(canonicalSlug) ?? null,
        canonicalSlug,
      } satisfies SeasonMergePlan;
    });
}

async function loadCompetitionSeasons(sql: Sql, seasonId: number) {
  return sql<CompetitionSeasonRow[]>`
    SELECT
      id,
      competition_id,
      current_matchday,
      total_matchdays,
      source_match_updated_at::text,
      source_match_available_at::text,
      source_match_updated_360_at::text,
      source_match_available_360_at::text,
      source_metadata,
      status,
      winner_team_id
    FROM competition_seasons
    WHERE season_id = ${seasonId}
    ORDER BY id ASC
  `;
}

async function hasOtherCurrentSeason(sql: Sql, seasonIds: number[]) {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS(
      SELECT 1
      FROM seasons
      WHERE is_current = TRUE
        AND id NOT IN ${sql(seasonIds)}
    ) AS exists
  `;

  return rows[0]?.exists ?? false;
}

async function ensureCanonicalSeason(tx: Sql, plan: SeasonMergePlan) {
  const hasConflictingCurrentSeason = await hasOtherCurrentSeason(
    tx,
    plan.canonical ? [plan.alias.id, plan.canonical.id] : [plan.alias.id],
  );

  if (plan.canonical) {
    const mergedStartDate = pickEarlierDate(plan.canonical.start_date, plan.alias.start_date);
    const mergedEndDate = pickLaterDate(plan.canonical.end_date, plan.alias.end_date);
    const mergedCurrent = isSeasonCurrentByDate(mergedStartDate, mergedEndDate) && !hasConflictingCurrentSeason;

    if (plan.alias.is_current && mergedCurrent) {
      await tx`
        UPDATE seasons
        SET is_current = FALSE
        WHERE id = ${plan.alias.id}
      `;
    }

    const rows = await tx<SeasonRow[]>`
      UPDATE seasons
      SET
        start_date = ${mergedStartDate},
        end_date = ${mergedEndDate},
        is_current = ${mergedCurrent}
      WHERE id = ${plan.canonical.id}
      RETURNING id, slug, start_date::text, end_date::text, is_current
    `;

    return rows[0];
  }

  const mergedCurrent = isSeasonCurrentByDate(plan.alias.start_date, plan.alias.end_date) && !hasConflictingCurrentSeason;
  const rows = await tx<SeasonRow[]>`
    UPDATE seasons
    SET
      slug = ${plan.canonicalSlug},
      is_current = ${mergedCurrent}
    WHERE id = ${plan.alias.id}
    RETURNING id, slug, start_date::text, end_date::text, is_current
  `;

  return rows[0];
}

async function mergeCompetitionSeason(tx: Sql, aliasRow: CompetitionSeasonRow, canonicalRow: CompetitionSeasonRow) {
  const mergedCurrentMatchday = Math.max(aliasRow.current_matchday ?? 0, canonicalRow.current_matchday ?? 0) || null;
  const mergedTotalMatchdays = Math.max(aliasRow.total_matchdays ?? 0, canonicalRow.total_matchdays ?? 0) || null;
  const mergedSourceMetadata = mergeMetadata(canonicalRow.source_metadata, aliasRow.source_metadata);

  await tx`
    UPDATE competition_seasons
    SET
      current_matchday = ${mergedCurrentMatchday},
      total_matchdays = ${mergedTotalMatchdays},
      source_match_updated_at = COALESCE(GREATEST(source_match_updated_at, ${aliasRow.source_match_updated_at}::timestamptz), source_match_updated_at, ${aliasRow.source_match_updated_at}::timestamptz),
      source_match_available_at = COALESCE(GREATEST(source_match_available_at, ${aliasRow.source_match_available_at}::timestamptz), source_match_available_at, ${aliasRow.source_match_available_at}::timestamptz),
      source_match_updated_360_at = COALESCE(GREATEST(source_match_updated_360_at, ${aliasRow.source_match_updated_360_at}::timestamptz), source_match_updated_360_at, ${aliasRow.source_match_updated_360_at}::timestamptz),
      source_match_available_360_at = COALESCE(GREATEST(source_match_available_360_at, ${aliasRow.source_match_available_360_at}::timestamptz), source_match_available_360_at, ${aliasRow.source_match_available_360_at}::timestamptz),
      source_metadata = ${JSON.stringify(mergedSourceMetadata)}::jsonb,
      status = ${mergeStatus(canonicalRow.status, aliasRow.status)},
      winner_team_id = COALESCE(winner_team_id, ${aliasRow.winner_team_id}),
      updated_at = NOW()
    WHERE id = ${canonicalRow.id}
  `;

  await tx`
    INSERT INTO team_seasons (team_id, competition_season_id, coach_id, created_at, updated_at)
    SELECT team_id, ${canonicalRow.id}, coach_id, created_at, NOW()
    FROM team_seasons
    WHERE competition_season_id = ${aliasRow.id}
    ON CONFLICT (team_id, competition_season_id)
    DO UPDATE SET
      coach_id = COALESCE(team_seasons.coach_id, EXCLUDED.coach_id),
      updated_at = NOW()
  `;

  await tx`
    DELETE FROM team_seasons
    WHERE competition_season_id = ${aliasRow.id}
  `;

  await tx`
    INSERT INTO player_contracts (
      player_id,
      team_id,
      competition_season_id,
      shirt_number,
      is_on_loan,
      joined_date,
      contract_start_date,
      contract_end_date,
      annual_salary_eur,
      weekly_wage_eur,
      salary_currency,
      salary_source,
      salary_source_url,
      salary_is_estimated,
      salary_updated_at,
      left_date,
      created_at,
      updated_at
    )
    SELECT
      player_id,
      team_id,
      ${canonicalRow.id},
      shirt_number,
      is_on_loan,
      joined_date,
      contract_start_date,
      contract_end_date,
      annual_salary_eur,
      weekly_wage_eur,
      salary_currency,
      salary_source,
      salary_source_url,
      salary_is_estimated,
      salary_updated_at,
      left_date,
      created_at,
      NOW()
    FROM player_contracts
    WHERE competition_season_id = ${aliasRow.id}
    ON CONFLICT (player_id, competition_season_id)
    DO UPDATE SET
      team_id = COALESCE(player_contracts.team_id, EXCLUDED.team_id),
      shirt_number = COALESCE(player_contracts.shirt_number, EXCLUDED.shirt_number),
      is_on_loan = player_contracts.is_on_loan OR EXCLUDED.is_on_loan,
      joined_date = COALESCE(player_contracts.joined_date, EXCLUDED.joined_date),
      contract_start_date = COALESCE(player_contracts.contract_start_date, EXCLUDED.contract_start_date),
      contract_end_date = COALESCE(player_contracts.contract_end_date, EXCLUDED.contract_end_date),
      annual_salary_eur = COALESCE(player_contracts.annual_salary_eur, EXCLUDED.annual_salary_eur),
      weekly_wage_eur = COALESCE(player_contracts.weekly_wage_eur, EXCLUDED.weekly_wage_eur),
      salary_currency = COALESCE(player_contracts.salary_currency, EXCLUDED.salary_currency),
      salary_source = COALESCE(player_contracts.salary_source, EXCLUDED.salary_source),
      salary_source_url = COALESCE(player_contracts.salary_source_url, EXCLUDED.salary_source_url),
      salary_is_estimated = player_contracts.salary_is_estimated AND EXCLUDED.salary_is_estimated,
      salary_updated_at = COALESCE(player_contracts.salary_updated_at, EXCLUDED.salary_updated_at),
      left_date = COALESCE(player_contracts.left_date, EXCLUDED.left_date),
      updated_at = NOW()
  `;

  await tx`
    DELETE FROM player_contracts
    WHERE competition_season_id = ${aliasRow.id}
  `;

  await tx`
    INSERT INTO player_season_stats (
      player_id,
      competition_season_id,
      appearances,
      starts,
      minutes_played,
      goals,
      assists,
      penalty_goals,
      own_goals,
      yellow_cards,
      red_cards,
      yellow_red_cards,
      clean_sheets,
      goals_conceded,
      saves,
      avg_rating,
      created_at,
      updated_at
    )
    SELECT
      player_id,
      ${canonicalRow.id},
      appearances,
      starts,
      minutes_played,
      goals,
      assists,
      penalty_goals,
      own_goals,
      yellow_cards,
      red_cards,
      yellow_red_cards,
      clean_sheets,
      goals_conceded,
      saves,
      avg_rating,
      created_at,
      NOW()
    FROM player_season_stats
    WHERE competition_season_id = ${aliasRow.id}
    ON CONFLICT (player_id, competition_season_id)
    DO UPDATE SET
      appearances = GREATEST(player_season_stats.appearances, EXCLUDED.appearances),
      starts = GREATEST(player_season_stats.starts, EXCLUDED.starts),
      minutes_played = GREATEST(player_season_stats.minutes_played, EXCLUDED.minutes_played),
      goals = GREATEST(player_season_stats.goals, EXCLUDED.goals),
      assists = GREATEST(player_season_stats.assists, EXCLUDED.assists),
      penalty_goals = GREATEST(player_season_stats.penalty_goals, EXCLUDED.penalty_goals),
      own_goals = GREATEST(player_season_stats.own_goals, EXCLUDED.own_goals),
      yellow_cards = GREATEST(player_season_stats.yellow_cards, EXCLUDED.yellow_cards),
      red_cards = GREATEST(player_season_stats.red_cards, EXCLUDED.red_cards),
      yellow_red_cards = GREATEST(player_season_stats.yellow_red_cards, EXCLUDED.yellow_red_cards),
      clean_sheets = GREATEST(player_season_stats.clean_sheets, EXCLUDED.clean_sheets),
      goals_conceded = GREATEST(player_season_stats.goals_conceded, EXCLUDED.goals_conceded),
      saves = GREATEST(player_season_stats.saves, EXCLUDED.saves),
      avg_rating = COALESCE(player_season_stats.avg_rating, EXCLUDED.avg_rating),
      updated_at = NOW()
  `;

  await tx`
    DELETE FROM player_season_stats
    WHERE competition_season_id = ${aliasRow.id}
  `;

  await tx`
    UPDATE matches
    SET competition_season_id = ${canonicalRow.id}, updated_at = NOW()
    WHERE competition_season_id = ${aliasRow.id}
  `;

  await tx`
    DELETE FROM competition_seasons
    WHERE id = ${aliasRow.id}
  `;
}

async function updateSeasonContexts(tx: Sql, aliasSlug: string, canonicalSlug: string) {
  const sourceEntityMappings = await tx`
    UPDATE source_entity_mapping
    SET season_context = ${canonicalSlug}, updated_at = NOW()
    WHERE season_context = ${aliasSlug}
  `;

  const rawPayloads = await tx`
    UPDATE raw_payloads
    SET season_context = ${canonicalSlug}
    WHERE season_context = ${aliasSlug}
  `;

  return Number(sourceEntityMappings.count ?? 0) + Number(rawPayloads.count ?? 0);
}

async function refreshMaterializedViews(sql: Sql, options: CliOptions) {
  if (options.dryRun || options.skipRefresh) {
    return;
  }

  await sql`REFRESH MATERIALIZED VIEW mv_team_form`;
  await sql`REFRESH MATERIALIZED VIEW mv_standings`;
  await sql`REFRESH MATERIALIZED VIEW mv_top_scorers`;
}

async function summarizePlan(sql: Sql, plan: SeasonMergePlan) {
  const competitionSeasonCount = await sql<{ count: number }[]>`
    SELECT COUNT(*)::int AS count
    FROM competition_seasons
    WHERE season_id = ${plan.alias.id}
  `;

  const sourceEntityMappingCount = await sql<{ count: number }[]>`
    SELECT COUNT(*)::int AS count
    FROM source_entity_mapping
    WHERE season_context = ${plan.alias.slug}
  `;

  const rawPayloadCount = await sql<{ count: number }[]>`
    SELECT COUNT(*)::int AS count
    FROM raw_payloads
    WHERE season_context = ${plan.alias.slug}
  `;

  return {
    aliasSeasonId: plan.alias.id,
    aliasSlug: plan.alias.slug,
    canonicalSeasonId: plan.canonical?.id ?? null,
    canonicalSlug: plan.canonicalSlug,
    aliasCompetitionSeasons: competitionSeasonCount[0]?.count ?? 0,
    aliasSourceEntityMappings: sourceEntityMappingCount[0]?.count ?? 0,
    aliasRawPayloads: rawPayloadCount[0]?.count ?? 0,
  };
}

async function executePlan(sql: ReturnType<typeof getSql>, plan: SeasonMergePlan) {
  return sql.begin(async (tx) => {
    const transactionSql = tx as unknown as Sql;
    const canonicalSeason = await ensureCanonicalSeason(transactionSql, plan);
    if (canonicalSeason.id === plan.alias.id) {
      const updatedSeasonContexts = await updateSeasonContexts(transactionSql, plan.alias.slug, canonicalSeason.slug);
      return {
        aliasSlug: plan.alias.slug,
        canonicalSlug: canonicalSeason.slug,
        canonicalSeasonId: canonicalSeason.id,
        aliasSeasonId: plan.alias.id,
        movedCompetitionSeasons: 0,
        mergedCompetitionSeasons: 0,
        updatedSeasonContexts,
        deletedAliasSeason: false,
      } satisfies MergeSummary;
    }

    const aliasCompetitionSeasons = await loadCompetitionSeasons(transactionSql, plan.alias.id);
    let movedCompetitionSeasons = 0;
    let mergedCompetitionSeasons = 0;

    for (const aliasCompetitionSeason of aliasCompetitionSeasons) {
      const canonicalCompetitionSeasons = await transactionSql<CompetitionSeasonRow[]>`
        SELECT
          id,
          competition_id,
          current_matchday,
          total_matchdays,
          source_match_updated_at::text,
          source_match_available_at::text,
          source_match_updated_360_at::text,
          source_match_available_360_at::text,
          source_metadata,
          status,
          winner_team_id
        FROM competition_seasons
        WHERE competition_id = ${aliasCompetitionSeason.competition_id}
          AND season_id = ${canonicalSeason.id}
      `;

      const canonicalCompetitionSeason = canonicalCompetitionSeasons[0];
      if (!canonicalCompetitionSeason) {
        await transactionSql`
          UPDATE competition_seasons
          SET season_id = ${canonicalSeason.id}, updated_at = NOW()
          WHERE id = ${aliasCompetitionSeason.id}
        `;
        movedCompetitionSeasons += 1;
        continue;
      }

      await mergeCompetitionSeason(transactionSql, aliasCompetitionSeason, canonicalCompetitionSeason);
      mergedCompetitionSeasons += 1;
    }

    const updatedSeasonContexts = await updateSeasonContexts(transactionSql, plan.alias.slug, canonicalSeason.slug);
    await transactionSql`DELETE FROM seasons WHERE id = ${plan.alias.id}`;

    return {
      aliasSlug: plan.alias.slug,
      canonicalSlug: canonicalSeason.slug,
      canonicalSeasonId: canonicalSeason.id,
      aliasSeasonId: plan.alias.id,
      movedCompetitionSeasons,
      mergedCompetitionSeasons,
      updatedSeasonContexts,
      deletedAliasSeason: true,
    } satisfies MergeSummary;
  });
}

async function main() {
  loadProjectEnv();
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const sql = getSql();

  try {
    const seasons = await loadSeasonRows(sql);
    const plans = buildMergePlans(seasons, options.onlySlug);

    if (options.onlySlug && plans.length === 0) {
      throw new Error(`No mergeable season found for slug: ${options.onlySlug}`);
    }

    const dryRunSummary = [];
    for (const plan of plans) {
      dryRunSummary.push(await summarizePlan(sql, plan));
    }

    if (options.dryRun) {
      console.log(JSON.stringify({ dryRun: true, mergePlans: dryRunSummary }, null, 2));
      return;
    }

    const results: MergeSummary[] = [];
    for (const plan of plans) {
      results.push(await executePlan(sql, plan));
    }

    await refreshMaterializedViews(sql, options);

    console.log(JSON.stringify({
      dryRun: false,
      mergedSeasonCount: results.length,
      results,
    }, null, 2));
  } finally {
    await sql.end({ timeout: 5 });
  }
}

await main();
