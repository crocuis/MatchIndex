import { writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  help: boolean;
  outputPath?: string;
  playerId?: string;
  playerUrl?: string;
}

interface TransferHistoryApiResponse {
  data?: {
    history?: {
      terminated?: Array<{
        details?: {
          age?: number;
          contractUntilDate?: string | null;
          date?: string | null;
          fee?: {
            compact?: { content?: string; prefix?: string; suffix?: string };
            currency?: string;
            value?: number | null;
          };
          isPending?: boolean;
          marketValue?: {
            currency?: string;
            value?: number | null;
          };
          season?: { display?: string };
        };
        id: string;
        relativeUrl?: string;
        transferDestination?: { clubId?: string };
        transferSource?: { clubId?: string };
        typeDetails?: { name?: string; type?: string; feeDescription?: string };
      }>;
    };
    playerId?: string;
  };
}

interface ClubApiResponse {
  data?: Array<{
    id: string;
    name: string;
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
  console.log(`Usage: node --experimental-strip-types scripts/fetch-player-transfers-transfermarkt.mts --player-id=<id> [options]

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

  const payload = await fetchJson<TransferHistoryApiResponse>(`${TM_API_BASE_URL}/transfer/history/player/${playerId}`);
  const rows = payload.data?.history?.terminated ?? [];
  const clubIds = [...new Set(rows.flatMap((entry) => [entry.transferSource?.clubId, entry.transferDestination?.clubId]).filter(Boolean))] as string[];
  const clubs = clubIds.length > 0
    ? await fetchJson<ClubApiResponse>(`${TM_API_BASE_URL}/clubs?${clubIds.map((id) => `ids[]=${encodeURIComponent(id)}`).join('&')}`)
    : { data: [] };

  const clubById = new Map((clubs.data ?? []).map((club) => [club.id, club.name]));
  const sourceUrl = options.playerUrl
    ? options.playerUrl.replace(/\/profil\/spieler\//, '/transfers/spieler/')
    : `https://www.transfermarkt.com/-/transfers/spieler/${playerId}`;

  const normalized = {
    fetchedAt: new Date().toISOString(),
    playerExternalId: payload.data?.playerId ?? playerId,
    provider: 'transfermarkt',
    rows: rows.map((entry) => ({
      age: entry.details?.age ?? null,
      contractUntilDate: entry.details?.contractUntilDate?.slice(0, 10) ?? null,
      currencyCode: entry.details?.fee?.currency || entry.details?.marketValue?.currency || 'EUR',
      externalTransferId: entry.id,
      fee: entry.details?.fee?.value ?? null,
      feeDisplay: [entry.details?.fee?.compact?.prefix, entry.details?.fee?.compact?.content, entry.details?.fee?.compact?.suffix]
        .filter(Boolean)
        .join('') || null,
      fromClubExternalId: entry.transferSource?.clubId ?? null,
      fromClubName: entry.transferSource?.clubId ? clubById.get(entry.transferSource.clubId) ?? null : null,
      isPending: entry.details?.isPending ?? false,
      marketValue: entry.details?.marketValue?.value ?? null,
      movedAt: entry.details?.date?.slice(0, 10) ?? null,
      seasonLabel: entry.details?.season?.display ?? null,
      sourceUrl: entry.relativeUrl ? `https://www.transfermarkt.com${entry.relativeUrl}` : sourceUrl,
      toClubExternalId: entry.transferDestination?.clubId ?? null,
      toClubName: entry.transferDestination?.clubId ? clubById.get(entry.transferDestination.clubId) ?? null : null,
      transferType: entry.typeDetails?.type ?? null,
      transferTypeLabel: entry.typeDetails?.name || entry.typeDetails?.feeDescription || null,
      raw: entry,
    })),
    sourceUrl,
  };

  const serialized = `${JSON.stringify(normalized, null, 2)}\n`;
  if (options.outputPath) {
    await writeFile(resolveOutputPath(options.outputPath), serialized, 'utf8');
    console.log(JSON.stringify({ outputPath: resolveOutputPath(options.outputPath), rows: normalized.rows.length }, null, 2));
    return;
  }

  process.stdout.write(serialized);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
