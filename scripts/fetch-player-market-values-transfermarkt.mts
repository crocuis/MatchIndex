import { writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  help: boolean;
  outputPath?: string;
  playerId?: string;
  playerUrl?: string;
}

interface MarketValueApiResponse {
  data?: {
    history?: Array<{
      age?: number;
      clubId?: string;
      marketValue?: {
        currency?: string;
        determined?: string;
        value?: number;
      };
      playerId?: string;
    }>;
  };
}

interface PlayerApiResponse {
  data?: Array<{
    id: string;
    name: string;
    relativeUrl?: string;
  }>;
}

interface ClubApiResponse {
  data?: Array<{
    id: string;
    name: string;
    relativeUrl?: string | null;
  }>;
}

const TM_API_BASE_URL = 'https://tmapi-alpha.transfermarkt.technology';

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = { help: false };
  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      options.help = true;
      continue;
    }
    if (arg.startsWith('--player-id=')) {
      options.playerId = arg.slice('--player-id='.length).trim();
      continue;
    }
    if (arg.startsWith('--player-url=')) {
      options.playerUrl = arg.slice('--player-url='.length).trim();
      continue;
    }
    if (arg.startsWith('--output=')) {
      options.outputPath = arg.slice('--output='.length).trim();
    }
  }
  return options;
}

function printHelp() {
  console.log(`Usage: node --experimental-strip-types scripts/fetch-player-market-values-transfermarkt.mts --player-id=<id> [options]

Options:
  --player-id=<id>     Transfermarkt player id
  --player-url=<url>   Full Transfermarkt player URL (alternative to --player-id)
  --output=<path>      Optional output JSON path
  --help, -h           Show this help message
`);
}

function resolvePlayerId(options: CliOptions) {
  if (options.playerId) {
    return options.playerId;
  }

  const match = options.playerUrl?.match(/\/spieler\/(\d+)/);
  return match?.[1];
}

function resolveOutputPath(filePath: string) {
  return path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
}

async function fetchJson<T>(url: string) {
  const response = await fetch(url, {
    headers: {
      accept: 'application/json',
      'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    },
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText} (${url})`);
  }

  return response.json() as Promise<T>;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) {
    printHelp();
    return;
  }

  const playerId = resolvePlayerId(options);
  if (!playerId) {
    throw new Error('--player-id or --player-url is required');
  }

  const [marketValues, players] = await Promise.all([
    fetchJson<MarketValueApiResponse>(`${TM_API_BASE_URL}/player/${playerId}/market-value-history`),
    fetchJson<PlayerApiResponse>(`${TM_API_BASE_URL}/players?ids[]=${playerId}`),
  ]);

  const history = marketValues.data?.history ?? [];
  const clubIds = [...new Set(history.map((entry) => entry.clubId).filter(Boolean))] as string[];
  const clubResponse = clubIds.length > 0
    ? await fetchJson<ClubApiResponse>(`${TM_API_BASE_URL}/clubs?${clubIds.map((id) => `ids[]=${encodeURIComponent(id)}`).join('&')}`)
    : { data: [] };

  const player = players.data?.[0];
  const clubById = new Map((clubResponse.data ?? []).map((club) => [club.id, club]));
  const relativeUrl = player?.relativeUrl ?? options.playerUrl ?? null;
  const sourceUrl = relativeUrl
    ? `https://www.transfermarkt.com${relativeUrl.replace(/\/profil\/spieler\//, '/marktwertverlauf/spieler/')}`
    : `https://www.transfermarkt.com/-/marktwertverlauf/spieler/${playerId}`;

  const payload = {
    fetchedAt: new Date().toISOString(),
    playerExternalId: playerId,
    playerName: player?.name ?? null,
    provider: 'transfermarkt',
    rows: history
      .filter((entry) => entry.marketValue?.value && entry.marketValue.determined)
      .map((entry) => ({
        age: entry.age ?? null,
        clubExternalId: entry.clubId ?? null,
        clubName: entry.clubId ? clubById.get(entry.clubId)?.name ?? null : null,
        currencyCode: entry.marketValue?.currency ?? 'EUR',
        marketValue: entry.marketValue?.value ?? null,
        observedAt: entry.marketValue?.determined?.slice(0, 10) ?? null,
        playerExternalId: entry.playerId ?? playerId,
        sourceUrl,
        raw: entry,
      })),
    sourceUrl,
  };

  const serialized = `${JSON.stringify(payload, null, 2)}\n`;
  if (options.outputPath) {
    await writeFile(resolveOutputPath(options.outputPath), serialized, 'utf8');
    console.log(JSON.stringify({ outputPath: resolveOutputPath(options.outputPath), rows: payload.rows.length }, null, 2));
    return;
  }

  process.stdout.write(serialized);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
