import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  help: boolean;
  inputPath?: string;
  limit?: number;
  outputPath?: string;
  writeMissing: boolean;
}

interface ExportTarget {
  fbrefUrl?: string;
  playerName: string;
  playerNames: string[];
  playerSlug: string;
  teamName: string;
  teamNames: string[];
}

interface ExportPayload {
  competitionSlug: string;
  seasonSlug: string;
  targets: ExportTarget[];
}

interface FbrefMappingEntry {
  playerSlug: string;
  sourceUrl: string;
}

interface CandidateRow {
  playerSlug: string;
  playerName: string;
  teamName: string;
  playerNames: string[];
  teamNames: string[];
  suggestedQuery: string;
  suggestedSearchUrl: string;
}

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
    if (arg.startsWith('--limit=')) {
      options.limit = parsePositiveInt(arg.slice('--limit='.length));
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/generate-fbref-player-mapping-candidates.mts --input=<path> [options]

Options:
  --input=<path>        Target JSON produced by export-player-contract-targets.mts
  --output=<path>       Write candidate JSON to a file
  --limit=<n>           Limit emitted candidates
  --write-missing       Append missing blank entries to fbref-player-mappings.json
  --help, -h            Show this help message

Environment:
  FBREF_PLAYER_MAPPINGS_FILE
                        Defaults to data/fbref-player-mappings.json
`);
}

function resolvePathLike(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

function resolveMappingsPath() {
  const configuredPath = process.env.FBREF_PLAYER_MAPPINGS_FILE?.trim();
  return resolvePathLike(configuredPath || 'data/fbref-player-mappings.json');
}

async function readJsonFile<T>(filePath: string) {
  return JSON.parse(await readFile(resolvePathLike(filePath), 'utf8')) as T;
}

async function loadMappings() {
  try {
    const payload = await readJsonFile<FbrefMappingEntry[]>(resolveMappingsPath());
    return payload;
  } catch {
    return [] as FbrefMappingEntry[];
  }
}

function buildSuggestedQuery(target: ExportTarget) {
  return `${target.playerName} ${target.teamName} site:fbref.com/en/players`;
}

function buildSearchUrl(query: string) {
  return `https://www.google.com/search?q=${encodeURIComponent(query)}`;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }
  if (!options.inputPath) {
    throw new Error('--input is required');
  }

  const payload = await readJsonFile<ExportPayload>(options.inputPath);
  const mappings = await loadMappings();
  const mappedPlayerSlugs = new Set(mappings.filter((entry) => entry.sourceUrl?.trim()).map((entry) => entry.playerSlug));

  const candidates = payload.targets
    .filter((target) => !target.fbrefUrl && !mappedPlayerSlugs.has(target.playerSlug))
    .slice(0, options.limit ?? payload.targets.length)
    .map<CandidateRow>((target) => {
      const suggestedQuery = buildSuggestedQuery(target);
      return {
        playerSlug: target.playerSlug,
        playerName: target.playerName,
        teamName: target.teamName,
        playerNames: target.playerNames,
        teamNames: target.teamNames,
        suggestedQuery,
        suggestedSearchUrl: buildSearchUrl(suggestedQuery),
      };
    });

  if (options.writeMissing && candidates.length > 0) {
    const existingPlayerSlugs = new Set(mappings.map((entry) => entry.playerSlug));
    const merged = [
      ...mappings,
      ...candidates
        .filter((candidate) => !existingPlayerSlugs.has(candidate.playerSlug))
        .map<FbrefMappingEntry>((candidate) => ({ playerSlug: candidate.playerSlug, sourceUrl: '' })),
    ];
    await writeFile(resolveMappingsPath(), `${JSON.stringify(merged, null, 2)}\n`, 'utf8');
  }

  const output = {
    competitionSlug: payload.competitionSlug,
    seasonSlug: payload.seasonSlug,
    totalTargets: payload.targets.length,
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
