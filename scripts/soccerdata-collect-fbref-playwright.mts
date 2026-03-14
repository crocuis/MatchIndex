import { mkdir, writeFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { chromium, type Page } from 'playwright';

interface LeagueConfig {
  competitionName: string;
  competitionSlug: string;
  league: string;
}

interface CollectorArgs {
  chromeChannel?: string;
  cookieFile?: string;
  competition: string;
  headed: boolean;
  output?: string;
  season: string;
  write: boolean;
}

interface FbrefRow {
  endpoint: string;
  entityType: 'competition' | 'match' | 'player' | 'team';
  externalId: string | null;
  externalParentId: string | null;
  manifestType: string;
  metadata: Record<string, unknown>;
  payload: Record<string, unknown>;
  seasonContext: string;
  sourceAvailableAt: string;
  sourceUpdatedAt: null;
  upstreamPath: string;
}

interface SeasonRow {
  format: string | null;
  seasonLabel: string;
  url: string;
}

const FBREF_BASE_URL = 'https://fbref.com';

const LEAGUE_BY_COMPETITION: Record<string, LeagueConfig> = {
  BL1: { competitionName: 'Bundesliga', competitionSlug: 'bundesliga', league: 'GER-Bundesliga' },
  FL1: { competitionName: 'Ligue 1', competitionSlug: 'ligue-1', league: 'FRA-Ligue 1' },
  PD: { competitionName: 'La Liga', competitionSlug: 'la-liga', league: 'ESP-La Liga' },
  PL: { competitionName: 'Premier League', competitionSlug: 'premier-league', league: 'ENG-Premier League' },
  SA: { competitionName: 'Serie A', competitionSlug: 'serie-a', league: 'ITA-Serie A' },
};

function parseArgs(argv: string[]): CollectorArgs {
  const getOption = (name: string) => {
    const match = argv.find((arg) => arg.startsWith(`--${name}=`));
    return match ? match.slice(name.length + 3) : undefined;
  };

  const competition = getOption('competition');
  const season = getOption('season');

  if (!competition || !season) {
    throw new Error('--competition and --season are required');
  }

  return {
    chromeChannel: getOption('chrome-channel')?.trim(),
    cookieFile: getOption('cookie-file')?.trim(),
    competition: competition.trim().toUpperCase(),
    headed: argv.includes('--headed'),
    output: getOption('output')?.trim(),
    season: season.trim(),
    write: argv.includes('--write'),
  };
}

function parseCookieHeader(cookieHeader: string) {
  return cookieHeader
    .split(';')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const separatorIndex = entry.indexOf('=');
      if (separatorIndex === -1) {
        return null;
      }

      return {
        domain: '.fbref.com',
        name: entry.slice(0, separatorIndex),
        path: '/',
        value: entry.slice(separatorIndex + 1),
      };
    })
    .filter((cookie): cookie is { domain: string; name: string; path: string; value: string } => Boolean(cookie));
}

function normalizeSeasonValue(value: string) {
  const trimmed = value.trim();
  return /^\d{4}$/.test(trimmed) ? `${trimmed}-${Number(trimmed) + 1}` : trimmed;
}

function buildEndpoint(league: string, season: string, dataset: string) {
  return `fbref-playwright://${league}/${season}/${dataset}`;
}

function makeRow(params: {
  competitionCode: string;
  dataset: string;
  entityType: FbrefRow['entityType'];
  externalId: string | null;
  externalParentId: string | null;
  fetchedAt: string;
  league: string;
  manifestType: string;
  payload: Record<string, unknown>;
  season: string;
}) {
  const endpoint = buildEndpoint(params.league, params.season, params.dataset);
  const row: FbrefRow = {
    endpoint,
    entityType: params.entityType,
    externalId: params.externalId,
    externalParentId: params.externalParentId,
    manifestType: params.manifestType,
    metadata: {
      competitionCode: params.competitionCode,
      dataset: params.dataset,
      league: params.league,
      season: params.season,
    },
    payload: params.payload,
    seasonContext: params.season,
    sourceAvailableAt: params.fetchedAt,
    sourceUpdatedAt: null,
    upstreamPath: endpoint,
  };

  return row;
}

async function waitForFbrefReady(page: Page, url: string) {
  await page.goto(url, { waitUntil: 'domcontentloaded' });
  for (let attempt = 0; attempt < 45; attempt += 1) {
    const title = await page.title();
    if (title && title !== 'Just a moment...') {
      return;
    }

    await page.waitForTimeout(1000);
  }

  throw new Error(`FBref challenge did not clear for ${url}`);
}

async function readLeagueRow(page: Page, competitionName: string) {
  const result = await page.evaluate((targetName) => {
    const tables = [...document.querySelectorAll<HTMLTableElement>("table[id*='comps']")];
    const rows = tables.flatMap((table) => [...table.querySelectorAll<HTMLTableRowElement>('tbody tr')]);
    for (const row of rows) {
      const cell = row.querySelector<HTMLElement>("th[data-stat='league_name'], td[data-stat='league_name']");
      const text = cell?.innerText.trim() ?? '';
      const href = cell?.querySelector<HTMLAnchorElement>('a')?.getAttribute('href') ?? '';
      if (text === targetName && href) {
        return {
          firstSeason: row.querySelector<HTMLElement>("td[data-stat='first_season']")?.innerText.trim() ?? null,
          lastSeason: row.querySelector<HTMLElement>("td[data-stat='last_season']")?.innerText.trim() ?? null,
          league: text,
          url: href,
        };
      }
    }
    return null;
  }, competitionName);

  if (!result?.url) {
    throw new Error(`League row not found for ${competitionName}`);
  }

  return result as Record<string, string | null> & { url: string };
}

async function readSeasonRow(page: Page, seasonInput: string) {
  const normalizedSeason = normalizeSeasonValue(seasonInput);
  const result = await page.evaluate((targetSeason) => {
    const table = document.querySelector<HTMLTableElement>('table#seasons');
    if (!table) {
      return null;
    }

    const rows = [...table.querySelectorAll<HTMLTableRowElement>('tbody tr')];
    for (const row of rows) {
      const cell = row.querySelector<HTMLElement>("th[data-stat='year_id'], th[data-stat='year'], td[data-stat='year_id'], td[data-stat='year']");
      const seasonLabel = cell?.innerText.trim() ?? '';
      const href = cell?.querySelector<HTMLAnchorElement>('a')?.getAttribute('href') ?? '';
      if (!href) {
        continue;
      }

      if (seasonLabel === targetSeason || seasonLabel.startsWith(targetSeason)) {
        return {
          format: row.querySelector("td[data-stat='final']") ? 'elimination' : 'round-robin',
          seasonLabel,
          url: href,
        };
      }
    }

    return null;
  }, normalizedSeason);

  if (!result?.url) {
    throw new Error(`Season row not found for ${seasonInput}`);
  }

  return result as SeasonRow;
}

async function extractTableRows(page: Page, selector: string) {
  const rows = await page.evaluate((tableSelector) => {
    const table = document.querySelector<HTMLTableElement>(tableSelector);
    if (!table) {
      return null;
    }

    return [...table.querySelectorAll<HTMLTableRowElement>('tbody tr')]
      .filter((row) => !row.classList.contains('thead'))
      .map((row) => {
        const record: Record<string, string> = {};
        for (const cell of row.querySelectorAll<HTMLElement>('th[data-stat], td[data-stat]')) {
          const key = cell.dataset.stat;
          if (!key) {
            continue;
          }

          record[key] = cell.innerText.trim();
          const href = cell.querySelector<HTMLAnchorElement>('a')?.getAttribute('href');
          if (href) {
            record[`${key}_href`] = href;
          }
        }
        return record;
      });
  }, selector);

  if (!rows) {
    throw new Error(`Table not found for selector: ${selector}`);
  }

  return rows as Record<string, string>[];
}

async function extractCommentTableRows(page: Page, tableId: string) {
  const rows = await page.evaluate((targetTableId) => {
    const comments: string[] = [];
    const walker = document.createTreeWalker(document.documentElement, NodeFilter.SHOW_COMMENT);
    while (walker.nextNode()) {
      comments.push(walker.currentNode.textContent ?? '');
    }

    for (const comment of comments) {
      if (!comment.includes(targetTableId)) {
        continue;
      }

      const doc = new DOMParser().parseFromString(comment, 'text/html');
      const table = doc.querySelector<HTMLTableElement>(`table#${targetTableId}`);
      if (!table) {
        continue;
      }

      return [...table.querySelectorAll<HTMLTableRowElement>('tbody tr')]
        .filter((row) => !row.classList.contains('thead'))
        .map((row) => {
          const record: Record<string, string> = {};
          for (const cell of row.querySelectorAll<HTMLElement>('th[data-stat], td[data-stat]')) {
            const key = cell.dataset.stat;
            if (!key) {
              continue;
            }

            record[key] = cell.innerText.trim();
            const href = cell.querySelector<HTMLAnchorElement>('a')?.getAttribute('href');
            if (href) {
              record[`${key}_href`] = href;
            }
          }
          return record;
        });
    }

    return null;
  }, tableId);

  if (!rows) {
    throw new Error(`Comment table not found for id: ${tableId}`);
  }

  return rows as Record<string, string>[];
}

async function readSchedulePath(page: Page) {
  const href = await page.evaluate(() => {
    const link = [...document.querySelectorAll<HTMLAnchorElement>('a')].find(
      (candidate) => candidate.textContent?.trim() === 'Scores & Fixtures',
    );
    return link?.getAttribute('href') ?? null;
  });

  if (!href) {
    throw new Error('Scores & Fixtures link not found');
  }

  return href;
}

function derivePlayerStatsPath(seasonPath: string) {
  const parts = seasonPath.split('/');
  const slug = parts.at(-1);
  if (!slug) {
    throw new Error(`Invalid season path: ${seasonPath}`);
  }

  return `${parts.slice(0, -1).join('/')}/stats/${slug}`;
}

async function collectRows(args: CollectorArgs) {
  const config = LEAGUE_BY_COMPETITION[args.competition];
  if (!config) {
    throw new Error(`Unsupported competition code '${args.competition}'`);
  }

  const browser = await chromium.launch({
    channel: args.chromeChannel === 'chrome' ? 'chrome' : undefined,
    headless: args.headed ? false : true,
  });
  const context = await browser.newContext({ locale: 'en-US' });
  if (args.cookieFile) {
    const cookieHeader = readFileSync(resolve(process.cwd(), args.cookieFile), 'utf8').trim();
    if (cookieHeader) {
      await context.addCookies(parseCookieHeader(cookieHeader).map((cookie) => ({
        ...cookie,
        httpOnly: false,
        sameSite: 'Lax' as const,
        secure: true,
      })));
    }
  }
  const page = await context.newPage();
  const fetchedAt = new Date().toISOString();
  const rows: FbrefRow[] = [];
  const datasetCounts: Record<string, number> = {};

  try {
    await waitForFbrefReady(page, `${FBREF_BASE_URL}/en/comps/`);
    const leagueRow = await readLeagueRow(page, config.competitionName);
    rows.push(makeRow({
      competitionCode: args.competition,
      dataset: 'league_info',
      entityType: 'competition',
      externalId: config.competitionSlug,
      externalParentId: args.competition,
      fetchedAt,
      league: config.league,
      manifestType: 'competition_batch',
      payload: leagueRow,
      season: args.season,
    }));
    datasetCounts.league_info = 1;

    await waitForFbrefReady(page, `${FBREF_BASE_URL}${leagueRow.url}`);
    const seasonRow = await readSeasonRow(page, args.season);
    rows.push(makeRow({
      competitionCode: args.competition,
      dataset: 'season_info',
      entityType: 'competition',
      externalId: args.season,
      externalParentId: args.competition,
      fetchedAt,
      league: config.league,
      manifestType: 'competition_season_batch',
      payload: { ...seasonRow },
      season: args.season,
    }));
    datasetCounts.season_info = 1;

    await waitForFbrefReady(page, `${FBREF_BASE_URL}${seasonRow.url}`);
    const teamRows = await extractTableRows(page, 'table#stats_squads_standard_for');
    datasetCounts.team_season_stats_standard = teamRows.length;
    for (const record of teamRows) {
      rows.push(makeRow({
        competitionCode: args.competition,
        dataset: 'team_season_stats_standard',
        entityType: 'team',
        externalId: record.squad || null,
        externalParentId: args.competition,
        fetchedAt,
        league: config.league,
        manifestType: 'team_season_batch',
        payload: record,
        season: args.season,
      }));
    }

    const schedulePath = await readSchedulePath(page);
    await waitForFbrefReady(page, `${FBREF_BASE_URL}${schedulePath}`);
    const scheduleRows = await extractTableRows(page, "table[id*='sched']");
    datasetCounts.schedule = scheduleRows.length;
    for (const record of scheduleRows) {
      const gameId = record.match_report_href?.split('/')[3] || record.date || null;
      rows.push(makeRow({
        competitionCode: args.competition,
        dataset: 'schedule',
        entityType: 'match',
        externalId: gameId,
        externalParentId: args.competition,
        fetchedAt,
        league: config.league,
        manifestType: 'match_batch',
        payload: record,
        season: args.season,
      }));
    }

    const playerStatsPath = derivePlayerStatsPath(seasonRow.url);
    await waitForFbrefReady(page, `${FBREF_BASE_URL}${playerStatsPath}`);
    const playerRows = await extractCommentTableRows(page, 'stats_standard');
    datasetCounts.player_season_stats_standard = playerRows.length;
    for (const record of playerRows) {
      if (record.player === 'Player') {
        continue;
      }

      const playerName = record.player?.trim() || '';
      const teamName = record.squad?.trim() || '';
      rows.push(makeRow({
        competitionCode: args.competition,
        dataset: 'player_season_stats_standard',
        entityType: 'player',
        externalId: playerName ? `${playerName}|${teamName}|${args.season}` : null,
        externalParentId: args.competition,
        fetchedAt,
        league: config.league,
        manifestType: 'player_season_batch',
        payload: record,
        season: args.season,
      }));
    }

    return {
      datasetCounts,
      fetchedAt,
      league: config.league,
      payloadCount: rows.length,
      rows,
    };
  } finally {
    await context.close();
    await browser.close();
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = LEAGUE_BY_COMPETITION[args.competition];
  if (!config) {
    throw new Error(`Unsupported competition code '${args.competition}'`);
  }

  if (!args.write) {
    console.log(JSON.stringify({
      collector: 'fbref_playwright',
      competition: args.competition,
      season: args.season,
      dryRun: true,
      headed: args.headed,
      implemented: true,
      league: config.league,
      cookieFileConfigured: Boolean(args.cookieFile),
      plannedDatasets: ['league_info', 'season_info', 'schedule', 'team_season_stats_standard', 'player_season_stats_standard'],
      outputPath: args.output ?? null,
      nextStep: 'Run with --write and --output to collect JSONL payloads through Playwright.',
    }));
    return;
  }

  if (!args.output) {
    throw new Error('--output is required when --write is used');
  }

  try {
    const result = await collectRows(args);
    const outputPath = resolve(process.cwd(), args.output);
    await mkdir(dirname(outputPath), { recursive: true });
    await writeFile(outputPath, `${result.rows.map((row) => JSON.stringify(row)).join('\n')}\n`, 'utf8');
    console.log(JSON.stringify({
      collector: 'fbref_playwright',
      competition: args.competition,
      cookieFileConfigured: Boolean(args.cookieFile),
      season: args.season,
      dryRun: false,
      headed: args.headed,
      implemented: true,
      league: result.league,
      datasetCounts: result.datasetCounts,
      payloadCount: result.payloadCount,
      fetchedAt: result.fetchedAt,
      outputPath: args.output,
    }));
  } catch (error) {
    console.log(JSON.stringify({
      collector: 'fbref_playwright',
      competition: args.competition,
      cookieFileConfigured: Boolean(args.cookieFile),
      season: args.season,
      dryRun: false,
      headed: args.headed,
      implemented: true,
      error: error instanceof Error ? error.message : String(error),
      hint: 'Playwright browser path failed. Check FBref challenge state or page structure changes.',
    }));
    process.exitCode = 1;
  }
}

await main();
