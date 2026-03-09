import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

type Provider = 'fbref' | 'transfermarkt';

interface CliOptions {
  help: boolean;
  inputPath?: string;
  limit?: number;
  outputPath?: string;
  provider?: Provider;
  writeMissing: boolean;
}

interface ExportTarget {
  fbrefUrl?: string;
  playerName: string;
  playerNames: string[];
  playerSlug: string;
  sourceUrl?: string;
  teamName: string;
  teamNames: string[];
}

interface ExportPayload {
  competitionSlug: string;
  seasonSlug: string;
  targets: ExportTarget[];
}

interface MappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

interface SearchCandidate {
  playerSlug: string;
  playerName: string;
  teamName: string;
  searchUrl: string;
  suggestedQuery: string;
  candidateUrls: string[];
}

const PROVIDER_CONFIG: Record<Provider, { domain: string; siteQuery: string; envVar: string; filePath: string; targetKey: 'fbrefUrl' | 'sourceUrl' }> = {
  fbref: {
    domain: 'fbref.com/en/players',
    siteQuery: 'fbref.com/en/players',
    envVar: 'FBREF_PLAYER_MAPPINGS_FILE',
    filePath: 'data/fbref-player-mappings.json',
    targetKey: 'fbrefUrl',
  },
  transfermarkt: {
    domain: 'transfermarkt.com',
    siteQuery: 'transfermarkt.com',
    envVar: 'TRANSFERMARKT_PLAYER_MAPPINGS_FILE',
    filePath: 'data/transfermarkt-player-mappings.json',
    targetKey: 'sourceUrl',
  },
};

function parsePositiveInt(value: string | undefined) {
  if (!value) return undefined;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false, writeMissing: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg === '--write-missing') {
      options.writeMissing = true;
      continue;
    }
    if (arg.startsWith('--input=')) {
      options.inputPath = arg.slice('--input='.length).trim();
      continue;
    }
    if (arg.startsWith('--output=')) {
      options.outputPath = arg.slice('--output='.length).trim();
      continue;
    }
    if (arg.startsWith('--provider=')) {
      const provider = arg.slice('--provider='.length).trim();
      if (provider === 'fbref' || provider === 'transfermarkt') {
        options.provider = provider;
      }
      continue;
    }
    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/search-player-profile-mappings.mts --provider=<fbref|transfermarkt> --input=<path> [options]

Options:
  --provider=<name>     fbref or transfermarkt
  --input=<path>        Target JSON produced by export-player-contract-targets.mts
  --output=<path>       Write search candidate JSON to a file
  --limit=<n>           Limit searched targets
  --write-missing       Append missing blank entries to the provider mapping file
  --help, -h            Show this help message
`);
}

function resolvePathLike(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

function resolveMappingsPath(provider: Provider) {
  const config = PROVIDER_CONFIG[provider];
  const configuredPath = process.env[config.envVar]?.trim();
  return resolvePathLike(configuredPath || config.filePath);
}

async function readJsonFile<T>(filePath: string) {
  return JSON.parse(await readFile(resolvePathLike(filePath), 'utf8')) as T;
}

async function loadMappings(provider: Provider) {
  try {
    return await readJsonFile<MappingEntry[]>(resolveMappingsPath(provider));
  } catch {
    return [] as MappingEntry[];
  }
}

function buildSuggestedQuery(provider: Provider, target: ExportTarget) {
  return `${target.playerName} ${target.teamName} site:${PROVIDER_CONFIG[provider].siteQuery}`;
}

function normalizeName(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}

function buildCandidateQueries(provider: Provider, target: ExportTarget) {
  const siteQuery = PROVIDER_CONFIG[provider].siteQuery;
  const names = Array.from(new Set([
    target.playerName,
    ...target.playerNames,
    normalizeName(target.playerName),
    ...target.playerNames.map(normalizeName),
  ].map((value) => value.trim()).filter(Boolean)));
  const teams = Array.from(new Set([
    target.teamName,
    ...target.teamNames,
    normalizeName(target.teamName),
    ...target.teamNames.map(normalizeName),
  ].map((value) => value.trim()).filter(Boolean)));

  const queries: string[] = [];
  for (const name of names) {
    for (const team of teams.slice(0, 2)) {
      queries.push(`${name} ${team} site:${siteQuery}`);
    }
    queries.push(`${name} site:${siteQuery}`);
  }

  return Array.from(new Set(queries));
}

function buildSearchUrl(query: string) {
  return `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
}

function decodeHtml(value: string) {
  return value
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function extractCandidateUrl(rawHref: string) {
  const decoded = decodeHtml(rawHref);
  const absolute = decoded.startsWith('//') ? `https:${decoded}` : decoded;

  try {
    const parsed = new URL(absolute);
    const redirected = parsed.searchParams.get('uddg');
    if (redirected) {
      return decodeURIComponent(redirected);
    }
  } catch {
    return absolute;
  }

  return absolute;
}

async function searchProviderUrls(provider: Provider, query: string) {
  const searchUrl = buildSearchUrl(query);
  const response = await fetch(searchUrl, {
    headers: {
      'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    },
  });
  if (!response.ok) {
    throw new Error(`Search request failed with ${response.status}`);
  }

  const html = await response.text();
  const matches = Array.from(html.matchAll(/<a[^>]+class="result__a"[^>]+href="([^"]+)"/g));
  const seen = new Set<string>();
  const urls: string[] = [];
  for (const match of matches) {
    const decoded = extractCandidateUrl(match[1]);
    if (!decoded.includes(PROVIDER_CONFIG[provider].domain)) {
      continue;
    }
    if (!seen.has(decoded)) {
      seen.add(decoded);
      urls.push(decoded);
    }
  }

  return { searchUrl, urls };
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  if (!options.inputPath || !options.provider) {
    throw new Error('--provider and --input are required');
  }

  const payload = await readJsonFile<ExportPayload>(options.inputPath);
  const mappings = await loadMappings(options.provider);
  const mappedPlayerSlugs = new Set(mappings.filter((entry) => entry.sourceUrl?.trim()).map((entry) => entry.playerSlug));
  const config = PROVIDER_CONFIG[options.provider];

  const candidates: SearchCandidate[] = [];
  const targets = payload.targets
    .filter((target) => !target[config.targetKey] && !mappedPlayerSlugs.has(target.playerSlug))
    .slice(0, options.limit ?? payload.targets.length);

  for (const target of targets) {
    const queries = buildCandidateQueries(options.provider, target);
    const suggestedQuery = queries[0] ?? buildSuggestedQuery(options.provider, target);
    let result = { searchUrl: buildSearchUrl(suggestedQuery), urls: [] as string[] };
    for (const query of queries) {
      result = await searchProviderUrls(options.provider, query);
      if (result.urls.length > 0) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 400));
    }
    candidates.push({
      candidateUrls: result.urls.slice(0, 5),
      playerName: target.playerName,
      playerSlug: target.playerSlug,
      searchUrl: result.searchUrl,
      suggestedQuery,
      teamName: target.teamName,
    });
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  if (options.writeMissing && candidates.length > 0) {
    const existingPlayerSlugs = new Set(mappings.map((entry) => entry.playerSlug));
    const merged = [
      ...mappings,
      ...candidates
        .filter((candidate) => !existingPlayerSlugs.has(candidate.playerSlug))
        .map<MappingEntry>((candidate) => ({ playerSlug: candidate.playerSlug, sourceUrl: '' })),
    ];
    await writeFile(resolveMappingsPath(options.provider), `${JSON.stringify(merged, null, 2)}\n`, 'utf8');
  }

  const output = {
    competitionSlug: payload.competitionSlug,
    provider: options.provider,
    seasonSlug: payload.seasonSlug,
    totalTargets: payload.targets.length,
    searchedTargets: targets.length,
    candidates,
  };
  const serialized = JSON.stringify(output, null, 2);
  if (options.outputPath) {
    await writeFile(resolvePathLike(options.outputPath), `${serialized}\n`, 'utf8');
  } else {
    console.log(serialized);
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
