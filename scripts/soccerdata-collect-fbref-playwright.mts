import { mkdir, writeFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { chromium, type BrowserContext, type Page } from 'playwright';

const DEFAULT_CHROME_CHANNEL = 'chrome';
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36';
const NAVIGATION_RETRY_LIMIT = 3;

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
  persistentProfileDir?: string;
  season: string;
  stayOpenSeconds?: number;
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
    persistentProfileDir: getOption('user-data-dir')?.trim(),
    season: season.trim(),
    stayOpenSeconds: Number.parseInt(getOption('stay-open-seconds') ?? '0', 10) || 0,
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

function getChromeChannel(value?: string) {
  const normalized = value?.trim().toLowerCase();
  return normalized === 'chrome' || normalized === 'msedge' ? normalized : DEFAULT_CHROME_CHANNEL;
}

function randomDelay(minMs: number, maxMs: number) {
  return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
}

function getBackoffDelay(attempt: number) {
  const base = Math.min(10_000, 1_500 * 2 ** attempt);
  return base + randomDelay(150, 900);
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
  let lastError: unknown = null;

  for (let navigationAttempt = 0; navigationAttempt < NAVIGATION_RETRY_LIMIT; navigationAttempt += 1) {
    try {
      await page.waitForTimeout(randomDelay(700, 1800));
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45_000 });
      await page.waitForTimeout(randomDelay(1200, 2600));

      for (let attempt = 0; attempt < 45; attempt += 1) {
        const title = await page.title();
        const challengeText = await page.locator('body').innerText().catch(() => '');

        if (title && title !== 'Just a moment...' && !challengeText.includes('Checking your browser')) {
          return;
        }

        await page.waitForTimeout(randomDelay(800, 1500));
      }

      throw new Error(`FBref challenge did not clear for ${url}`);
    } catch (error) {
      lastError = error;
      if (navigationAttempt < NAVIGATION_RETRY_LIMIT - 1) {
        await page.waitForTimeout(getBackoffDelay(navigationAttempt));
        continue;
      }
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`FBref challenge did not clear for ${url}`);
}

async function applyStealth(contextPage: Page) {
  await contextPage.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
    Object.defineProperty(navigator, 'language', { get: () => 'en-US' });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 8 });
    Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
    Object.defineProperty(navigator, 'plugins', {
      get: () => [
        { name: 'Chrome PDF Plugin' },
        { name: 'Chrome PDF Viewer' },
        { name: 'Native Client' },
      ],
    });

    const originalQuery = window.navigator.permissions?.query?.bind(window.navigator.permissions);
    if (originalQuery) {
      window.navigator.permissions.query = (parameters: PermissionDescriptor) => (
        parameters.name === 'notifications'
          ? Promise.resolve({ state: Notification.permission } as PermissionStatus)
          : originalQuery(parameters)
      );
    }
  });
}

async function createBrowserContext(args: CollectorArgs) {
  const launchArgs = [
    '--disable-blink-features=AutomationControlled',
    '--disable-dev-shm-usage',
    '--no-default-browser-check',
    '--disable-infobars',
  ];

  const contextOptions = {
    colorScheme: 'light' as const,
    deviceScaleFactor: 1,
    extraHTTPHeaders: {
      'Accept-Language': 'en-US,en;q=0.9',
      'Cache-Control': 'max-age=0',
      DNT: '1',
      'Upgrade-Insecure-Requests': '1',
    },
    locale: 'en-US',
    timezoneId: 'America/New_York',
    userAgent: DEFAULT_USER_AGENT,
    viewport: { width: 1440, height: 900 },
  };

  if (args.persistentProfileDir) {
    return chromium.launchPersistentContext(resolve(process.cwd(), args.persistentProfileDir), {
      ...contextOptions,
      channel: getChromeChannel(args.chromeChannel),
      headless: args.headed ? false : true,
      args: launchArgs,
    });
  }

  const browser = await chromium.launch({
    channel: getChromeChannel(args.chromeChannel),
    headless: args.headed ? false : true,
    args: launchArgs,
  });

  return browser.newContext(contextOptions);
}

async function getActivePage(context: BrowserContext) {
  const existingPage = context.pages()[0];
  return existingPage ?? context.newPage();
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

  const context = await createBrowserContext(args);
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
  const page = await getActivePage(context);
  await applyStealth(page);
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
    if (args.headed && args.stayOpenSeconds && args.stayOpenSeconds > 0) {
      await page.waitForTimeout(args.stayOpenSeconds * 1000);
    }

    await context.close();
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
        persistentProfileDir: args.persistentProfileDir ?? null,
        plannedDatasets: ['league_info', 'season_info', 'schedule', 'team_season_stats_standard', 'player_season_stats_standard'],
        outputPath: args.output ?? null,
        nextStep: 'Run with --write and --output to collect JSONL payloads through Playwright. For Cloudflare-heavy flows, prefer --headed with --user-data-dir.',
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
      persistentProfileDir: args.persistentProfileDir ?? null,
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
      persistentProfileDir: args.persistentProfileDir ?? null,
      error: error instanceof Error ? error.message : String(error),
      hint: 'Playwright browser path failed. Prefer --headed with --user-data-dir and manually clear any remaining challenge before rerunning.',
    }));
    process.exitCode = 1;
  }
}

await main();
