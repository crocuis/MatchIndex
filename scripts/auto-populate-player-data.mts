import { execFile } from 'node:child_process';
import { access, readFile } from 'node:fs/promises';
import { promisify } from 'node:util';
import path from 'node:path';

const execFileAsync = promisify(execFile);

interface CliOptions {
  capologyLeague?: string;
  capologySeason?: string;
  competition?: string;
  help: boolean;
  limit?: number;
  player?: string;
  season?: string;
  team?: string;
  skipFbref: boolean;
  skipEnsureSchema: boolean;
  skipSearchMappings: boolean;
  skipTransferHistory: boolean;
}

interface ExportTarget {
  playerSlug: string;
  sourceUrl?: string;
}

interface ExportPayload {
  targets: ExportTarget[];
}

const SCRAPERFC_TRANSFERMARKT_LEAGUES: Record<string, string> = {
  '1-bundesliga': 'Germany Bundesliga',
  'la-liga': 'Spain La Liga',
  'ligue-1': 'France Ligue 1',
  'premier-league': 'England Premier League',
  'serie-a': 'Italy Serie A',
};

function parsePositiveInt(value: string | undefined) {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false, skipEnsureSchema: false, skipFbref: false, skipSearchMappings: false, skipTransferHistory: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--skip-ensure-schema') {
      options.skipEnsureSchema = true;
      continue;
    }
    if (arg === '--skip-fbref') {
      options.skipFbref = true;
      continue;
    }
    if (arg === '--skip-search-mappings') {
      options.skipSearchMappings = true;
      continue;
    }
    if (arg === '--skip-transfer-history') {
      options.skipTransferHistory = true;
      continue;
    }
    if (arg.startsWith('--competition=')) { options.competition = arg.slice('--competition='.length).trim(); continue; }
    if (arg.startsWith('--season=')) { options.season = arg.slice('--season='.length).trim(); continue; }
    if (arg.startsWith('--capology-league=')) { options.capologyLeague = arg.slice('--capology-league='.length).trim(); continue; }
    if (arg.startsWith('--capology-season=')) { options.capologySeason = arg.slice('--capology-season='.length).trim(); continue; }
    if (arg.startsWith('--player=')) { options.player = arg.slice('--player='.length).trim(); continue; }
    if (arg.startsWith('--team=')) { options.team = arg.slice('--team='.length).trim(); continue; }
    if (arg.startsWith('--limit=')) { options.limit = parsePositiveInt(arg.slice('--limit='.length)); }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/auto-populate-player-data.mts --competition=<slug> --season=<slug> [options]

Options:
  --competition=<slug>  Internal competition slug
  --season=<slug>       Internal season slug
  --capology-league=<name>  Optional ScraperFC Capology league label
  --capology-season=<name>  Optional ScraperFC Capology season label (e.g. 2025-26)
  --team=<slug>         Optional single team slug
  --player=<slug>       Optional single player slug
  --limit=<n>           Limit exported targets
  --skip-search-mappings  Skip mapping search/apply steps
  --skip-fbref          Skip FBref fetch/sync steps
  --skip-transfer-history  Skip Transfermarkt transfer history fetch/sync steps
  --skip-ensure-schema  Skip schema preparation step
  --help, -h            Show this help message
`);
}

function getBaseName(competition: string, season: string, player?: string) {
  const safeSeason = season.replace(/[\\/]/g, '-');
  const suffix = player ? `${competition}-${safeSeason}-${player}` : `${competition}-${safeSeason}`;
  return path.join(process.cwd(), 'data', suffix);
}

function toScraperfcSeason(season: string) {
  const match = season.match(/^(\d{4})[\/-](\d{2})$/);
  if (match) {
    return `${match[1].slice(-2)}/${match[2]}`;
  }

  return season;
}

async function resolvePythonCommand() {
  const configuredPath = process.env.SCRAPERFC_PYTHON_PATH?.trim();
  const candidates = [
    configuredPath,
    path.join(process.cwd(), '.venv-scraperfc', 'bin', 'python'),
  ].filter((value): value is string => Boolean(value));

  for (const candidate of candidates) {
    try {
      await access(candidate);
      return candidate;
    } catch {
      continue;
    }
  }

  return 'python3';
}

async function run(command: string, args: string[], label: string) {
  const { stdout, stderr } = await execFileAsync(command, args, { cwd: process.cwd(), maxBuffer: 10 * 1024 * 1024 });
  if (stdout.trim()) {
    console.log(`[${label}] stdout\n${stdout.trim()}`);
  }
  if (stderr.trim()) {
    console.log(`[${label}] stderr\n${stderr.trim()}`);
  }
}

async function runBestEffort(command: string, args: string[], label: string) {
  try {
    await run(command, args, label);
  } catch (error) {
    console.log(`[${label}] skipped ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  if (!options.competition || !options.season) {
    throw new Error('--competition and --season are required');
  }

  const baseName = getBaseName(options.competition, options.season, options.player);
  const targetsPath = `${baseName}-targets.json`;
  const fbrefCandidatesPath = `${baseName}-fbref-search.json`;
  const transfermarktCandidatesPath = `${baseName}-transfermarkt-search.json`;
  const profilesPath = `${baseName}-fbref-profiles.json`;
  const contractsPath = `${baseName}-transfermarkt-contracts.json`;
  const capologyPath = `${baseName}-capology-contracts.json`;
  const transfersDir = `${baseName}-transfer-history`;
  const pythonCommand = await resolvePythonCommand();

  if (!options.skipEnsureSchema) {
    await run('node', ['--experimental-strip-types', 'scripts/ensure-player-enrichment-schema.mts'], 'ensure-schema');
  }

  const exportArgs = ['--experimental-strip-types', 'scripts/export-player-contract-targets.mts', `--competition=${options.competition}`, `--season=${options.season}`, `--output=${targetsPath}`];
  if (options.limit) exportArgs.push(`--limit=${options.limit}`);
  if (options.player) exportArgs.push(`--player=${options.player}`);
  if (options.team) exportArgs.push(`--team=${options.team}`);
  await run('node', exportArgs, 'export-targets');

  if (!options.skipSearchMappings) {
    for (const provider of ['fbref', 'transfermarkt'] as const) {
      const outputPath = provider === 'fbref' ? fbrefCandidatesPath : transfermarktCandidatesPath;
      await runBestEffort('node', ['--experimental-strip-types', 'scripts/search-player-profile-mappings.mts', `--provider=${provider}`, `--input=${targetsPath}`, `--output=${outputPath}`, ...(options.limit ? [`--limit=${options.limit}`] : [])], `${provider}-search`);
      await runBestEffort('node', ['--experimental-strip-types', 'scripts/apply-player-profile-search-candidates.mts', `--provider=${provider}`, `--input=${outputPath}`], `${provider}-apply`);
    }
  }

  await run('node', exportArgs, 'export-targets-refresh');

  let targetsPayload = JSON.parse(await readFile(targetsPath, 'utf8')) as ExportPayload;

  if (!options.skipFbref) {
    await runBestEffort(pythonCommand, ['scripts/fetch-player-profiles-fbref.py', `--targets=${targetsPath}`, `--output=${profilesPath}`, ...(options.limit ? [`--limit=${options.limit}`] : [])], 'fbref-fetch');
    await runBestEffort('node', ['--experimental-strip-types', 'scripts/sync-player-profiles.mts', `--input=${profilesPath}`], 'fbref-sync');
  }

  await run(pythonCommand, ['scripts/fetch-player-contracts-transfermarkt.py', `--targets=${targetsPath}`, `--output=${contractsPath}`, ...(options.limit ? [`--limit=${options.limit}`] : [])], 'transfermarkt-fetch');
  await run('node', ['--experimental-strip-types', 'scripts/sync-player-contracts.mts', `--input=${contractsPath}`, `--competition=${options.competition}`, `--season=${options.season}`, ...(options.player ? [`--player=${options.player}`] : [])], 'transfermarkt-sync');

  if (targetsPayload.targets.some((target) => !target.sourceUrl)) {
    const scraperfcLeague = SCRAPERFC_TRANSFERMARKT_LEAGUES[options.competition];
    if (scraperfcLeague) {
      await runBestEffort(pythonCommand, [
        'scripts/fetch-player-contracts-scraperfc.py',
        'transfermarkt-league',
        `--league=${scraperfcLeague}`,
        `--season=${toScraperfcSeason(options.season)}`,
        `--targets=${targetsPath}`,
        `--output=${contractsPath}`,
        ...(options.limit ? [`--limit=${options.limit}`] : []),
      ], 'transfermarkt-scraperfc-fetch');
      await runBestEffort('node', [
        '--experimental-strip-types',
        'scripts/sync-player-contracts.mts',
        `--input=${contractsPath}`,
        `--competition=${options.competition}`,
        `--season=${options.season}`,
        ...(options.team ? [`--team=${options.team}`] : []),
        ...(options.player ? [`--player=${options.player}`] : []),
        ...(options.limit ? [`--limit=${options.limit}`] : []),
      ], 'transfermarkt-scraperfc-sync');
      await run('node', exportArgs, 'export-targets-transfermarkt-refresh');
      targetsPayload = JSON.parse(await readFile(targetsPath, 'utf8')) as ExportPayload;
    }
  }

  if (!options.skipTransferHistory) {
    let transferRuns = 0;
    for (const target of targetsPayload.targets.filter((entry) => entry.sourceUrl)) {
      const outputPath = `${transfersDir}-${target.playerSlug}.json`;
      await runBestEffort('node', [
        '--experimental-strip-types',
        'scripts/fetch-player-transfers-transfermarkt.mts',
        `--player-url=${target.sourceUrl}`,
        `--output=${outputPath}`,
      ], `transfermarkt-transfer-fetch:${target.playerSlug}`);
      await runBestEffort('node', [
        '--experimental-strip-types',
        'scripts/sync-player-transfers.mts',
        `--input=${outputPath}`,
        `--player=${target.playerSlug}`,
      ], `transfermarkt-transfer-sync:${target.playerSlug}`);
      transferRuns += 1;
    }
    console.log(`[transfermarkt-transfer-history] processed ${transferRuns} target(s)`);
  }

  if (options.capologyLeague && options.capologySeason) {
    await runBestEffort(pythonCommand, [
      'scripts/fetch-player-contracts-scraperfc.py',
      'capology-league',
      `--league=${options.capologyLeague}`,
      `--season=${options.capologySeason}`,
      '--currency=eur',
      `--targets=${targetsPath}`,
      `--output=${capologyPath}`,
    ], 'capology-fetch');
    await runBestEffort('node', [
      '--experimental-strip-types',
      'scripts/sync-player-contracts.mts',
      `--input=${capologyPath}`,
      `--competition=${options.competition}`,
      `--season=${options.season}`,
      ...(options.team ? [`--team=${options.team}`] : []),
      ...(options.player ? [`--player=${options.player}`] : []),
    ], 'capology-sync');
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
