import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

type Provider = 'fbref' | 'transfermarkt';

interface CliOptions {
  help: boolean;
  inputPath?: string;
  outputPath?: string;
  provider?: Provider;
}

interface SearchCandidate {
  playerSlug: string;
  candidateUrls: string[];
}

interface SearchPayload {
  candidates: SearchCandidate[];
  provider: Provider;
}

interface MappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

const PROVIDER_CONFIG: Record<Provider, { envVar: string; filePath: string; matcher: RegExp }> = {
  fbref: {
    envVar: 'FBREF_PLAYER_MAPPINGS_FILE',
    filePath: 'data/fbref-player-mappings.json',
    matcher: /^https:\/\/fbref\.com\/en\/players\/[^/]+\/[^/?#]+\/?$/i,
  },
  transfermarkt: {
    envVar: 'TRANSFERMARKT_PLAYER_MAPPINGS_FILE',
    filePath: 'data/transfermarkt-player-mappings.json',
    matcher: /^https:\/\/www\.transfermarkt\.com\/[^/]+\/profil\/spieler\/\d+$/i,
  },
};

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
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
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/apply-player-profile-search-candidates.mts --provider=<fbref|transfermarkt> --input=<path> [options]

Options:
  --provider=<name>     fbref or transfermarkt
  --input=<path>        Search candidate JSON produced by search-player-profile-mappings.mts
  --output=<path>       Optional output mapping file path
  --help, -h            Show this help message
`);
}

function resolvePathLike(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

function resolveMappingsPath(provider: Provider, overridePath?: string) {
  if (overridePath) {
    return resolvePathLike(overridePath);
  }
  const config = PROVIDER_CONFIG[provider];
  const configuredPath = process.env[config.envVar]?.trim();
  return resolvePathLike(configuredPath || config.filePath);
}

async function readJsonFile<T>(filePath: string) {
  return JSON.parse(await readFile(resolvePathLike(filePath), 'utf8')) as T;
}

async function loadMappings(filePath: string) {
  try {
    return await readJsonFile<MappingEntry[]>(filePath);
  } catch {
    return [] as MappingEntry[];
  }
}

function pickBestCandidate(provider: Provider, urls: string[]) {
  const matcher = PROVIDER_CONFIG[provider].matcher;
  return urls.find((url) => matcher.test(url)) ?? urls[0] ?? '';
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

  const payload = await readJsonFile<SearchPayload>(options.inputPath);
  const outputPath = resolveMappingsPath(options.provider, options.outputPath);
  const existing = await loadMappings(outputPath);
  const existingByPlayer = new Map(existing.map((entry) => [entry.playerSlug, entry.sourceUrl]));

  for (const candidate of payload.candidates) {
    if (existingByPlayer.get(candidate.playerSlug)?.trim()) {
      continue;
    }
    const selected = pickBestCandidate(options.provider, candidate.candidateUrls);
    if (selected) {
      existingByPlayer.set(candidate.playerSlug, selected);
    }
  }

  const merged = Array.from(existingByPlayer.entries())
    .map<MappingEntry>(([playerSlug, sourceUrl]) => ({ playerSlug, sourceUrl }))
    .sort((left, right) => left.playerSlug.localeCompare(right.playerSlug));
  await writeFile(outputPath, `${JSON.stringify(merged, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({ provider: options.provider, outputPath, written: merged.filter((entry) => entry.sourceUrl).length }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
