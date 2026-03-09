import { writeFile } from 'node:fs/promises';

const GROUPS_URL = 'https://raw.githubusercontent.com/openfootball/worldcup/master/2026--united-states/cup.txt';
const FINALS_URL = 'https://raw.githubusercontent.com/openfootball/worldcup/master/2026--united-states/cup_finals.txt';
const OUTPUT_PATH = new URL('../data/worldcup-2026.json', import.meta.url);

const LOCAL_NATION_IDS: Record<string, string> = {
  Brazil: 'bra',
  England: 'eng',
  France: 'fra',
  Germany: 'ger',
  Italy: 'ita',
  Netherlands: 'ned',
  Portugal: 'por',
  'Saudi Arabia': 'sau',
  Spain: 'esp',
};

const TEAM_CODES: Record<string, string> = {
  Algeria: 'ALG',
  Argentina: 'ARG',
  Australia: 'AUS',
  Austria: 'AUT',
  Belgium: 'BEL',
  Brazil: 'BRA',
  Canada: 'CAN',
  'Cape Verde': 'CPV',
  Colombia: 'COL',
  Croatia: 'CRO',
  Curaçao: 'CUW',
  Ecuador: 'ECU',
  Egypt: 'EGY',
  England: 'ENG',
  France: 'FRA',
  Germany: 'GER',
  Ghana: 'GHA',
  Haiti: 'HAI',
  Iran: 'IRN',
  'Ivory Coast': 'CIV',
  Japan: 'JPN',
  Jordan: 'JOR',
  Mexico: 'MEX',
  Morocco: 'MAR',
  Netherlands: 'NED',
  'New Zealand': 'NZL',
  Norway: 'NOR',
  Panama: 'PAN',
  Paraguay: 'PAR',
  Portugal: 'POR',
  Qatar: 'QAT',
  'Saudi Arabia': 'KSA',
  Scotland: 'SCO',
  Senegal: 'SEN',
  'South Africa': 'RSA',
  'South Korea': 'KOR',
  Spain: 'ESP',
  Switzerland: 'SUI',
  Tunisia: 'TUN',
  Uruguay: 'URU',
  Uzbekistan: 'UZB',
  'United States': 'USA',
  'UEFA Path A winner': 'UPA',
  'UEFA Path B winner': 'UPB',
  'UEFA Path C winner': 'UPC',
  'UEFA Path D winner': 'UPD',
  'IC Path 1 winner': 'ICP',
  'IC Path 2 winner': 'IC2',
};

const SPOTLIGHTS: Record<string, { playerId: string; note: string }> = {
  bra: { playerId: 'p-rma-cwc-1', note: 'Left-side carrying threat and the cleanest route into transition attacks.' },
  eng: { playerId: 'p-bay-1', note: 'Reference point in the box and still the safest finisher in decisive phases.' },
  esp: { playerId: 'p-bar-4', note: 'Creates overloads wide and changes settled games with 1v1 progression.' },
  fra: { playerId: 'p-rma-5', note: 'Most direct match-winner in open field and still the key final-third runner.' },
  ger: { playerId: 'p-b04-1', note: 'Primary line-breaking creator and the best rhythm controller between the lines.' },
  ita: { playerId: 'p-int-1', note: 'Penalty-box efficiency gives Italy more value than their shot volume suggests.' },
  ned: { playerId: 'p-lee-1', note: 'Vertical running and set-piece presence make him a tournament swing player.' },
  por: { playerId: 'p-mun-uel-1', note: 'Sets the team tempo and carries the highest creative burden in possession.' },
};

type WorldCupMatch = {
  id: string;
  homeTeamId: string;
  awayTeamId: string;
  homeTeamName: string;
  awayTeamName: string;
  homeTeamCode: string;
  awayTeamCode: string;
  homeScore: null;
  awayScore: null;
  date: string;
  time: string;
  venue: string;
  leagueId: string;
  competitionName: string;
  teamType: 'nation';
  status: 'scheduled';
};

function slugify(value: string) {
  return value
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function toNationId(name: string) {
  return LOCAL_NATION_IDS[name] ?? slugify(name);
}

function toNationCode(name: string) {
  return TEAM_CODES[name] ?? slugify(name).replace(/-/g, '').slice(0, 3).toUpperCase();
}

function parseTeamsSegment(raw: string) {
  return raw.split(/\t+| {2,}/).map((value) => value.trim()).filter(Boolean);
}

function parseIsoDate(raw: string) {
  const normalized = raw.replace(/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+/, '').trim();
  const parts = normalized.match(/([A-Za-z]+)\s+(\d{1,2})/);
  if (!parts) {
    throw new Error(`Unable to parse date segment: ${raw}`);
  }

  const monthMap: Record<string, string> = {
    January: '01',
    Jan: '01',
    February: '02',
    Feb: '02',
    March: '03',
    Mar: '03',
    April: '04',
    Apr: '04',
    May: '05',
    June: '06',
    Jun: '06',
    July: '07',
    Jul: '07',
    August: '08',
    Aug: '08',
    September: '09',
    Sep: '09',
    October: '10',
    Oct: '10',
    November: '11',
    Nov: '11',
    December: '12',
    Dec: '12',
  };

  const month = monthMap[parts[1]];
  const day = parts[2].padStart(2, '0');
  if (!month) {
    throw new Error(`Unknown month: ${parts[1]}`);
  }

  return `2026-${month}-${day}`;
}

function buildMatchId(prefix: string, index: number) {
  return `${prefix}${String(index).padStart(3, '0')}`;
}

function parseGroupFile(source: string) {
  const lines = source.split(/\r?\n/);
  const groups: Array<{ id: string; name: string; standings: Array<Record<string, unknown>> }> = [];
  const matches: WorldCupMatch[] = [];

  let currentDate = '';
  let inGroupMatches = false;
  let groupMatchIndex = 1;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const groupHeader = trimmed.match(/^Group\s+([A-L])\s+\|\s+(.+)$/);
    if (groupHeader) {
      const groupLetter = groupHeader[1];
      const teams = parseTeamsSegment(groupHeader[2]);
      groups.push({
        id: `group-${groupLetter.toLowerCase()}`,
        name: `Group ${groupLetter}`,
        standings: teams.map((team, index) => ({
          position: index + 1,
          nationId: toNationId(team),
          nationName: team,
          nationCode: toNationCode(team),
          played: 0,
          won: 0,
          drawn: 0,
          lost: 0,
          goalsFor: 0,
          goalsAgainst: 0,
          goalDifference: 0,
          points: 0,
          form: [],
        })),
      });
      continue;
    }

    if (trimmed.startsWith('▪ Group ')) {
      inGroupMatches = true;
      currentDate = '';
      continue;
    }

    if (!inGroupMatches) continue;
    if (trimmed.startsWith('▪ ')) continue;

    if (/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|June|July)\b/.test(trimmed)) {
      currentDate = parseIsoDate(trimmed);
      continue;
    }

    const matchLine = trimmed.match(/^(?:\((\d+)\)\s+)?(\d{1,2}:\d{2})\s+UTC[^\s]+\s+(.+?)\s+v\s+(.+?)\s+@\s+(.+)$/);
    if (!matchLine || !currentDate) continue;

    const homeTeamName = matchLine[3].trim();
    const awayTeamName = matchLine[4].trim();
    matches.push({
      id: buildMatchId('m-wc26-g', groupMatchIndex++),
      homeTeamId: toNationId(homeTeamName),
      awayTeamId: toNationId(awayTeamName),
      homeTeamName,
      awayTeamName,
      homeTeamCode: toNationCode(homeTeamName),
      awayTeamCode: toNationCode(awayTeamName),
      homeScore: null,
      awayScore: null,
      date: currentDate,
      time: matchLine[2],
      venue: matchLine[5].trim(),
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'scheduled',
    });
  }

  return { groups, matches };
}

function parseFinalsFile(source: string) {
  const lines = source.split(/\r?\n/);
  const stages: Array<{ name: string; matchIds: string[] }> = [];
  const matches: WorldCupMatch[] = [];

  let currentStage = '';
  let currentDate = '';
  let knockoutIndex = 1;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    if (trimmed.startsWith('▪ ')) {
      currentStage = trimmed.replace(/^▪\s+/, '').trim();
      stages.push({ name: currentStage, matchIds: [] });
      currentDate = '';
      continue;
    }

    if (/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun|June|July)\b/.test(trimmed)) {
      currentDate = parseIsoDate(trimmed);
      continue;
    }

    const matchLine = trimmed.match(/^(?:\((\d+)\)\s+)?(\d{1,2}:\d{2})\s+UTC[^\s]+\s+(.+?)\s+v\s+(.+?)\s+@\s+(.+)$/);
    if (!matchLine || !currentDate || stages.length === 0) continue;

    const homeTeamName = matchLine[3].trim();
    const awayTeamName = matchLine[4].trim();
    const id = buildMatchId('m-wc26-k', knockoutIndex++);

    matches.push({
      id,
      homeTeamId: toNationId(homeTeamName),
      awayTeamId: toNationId(awayTeamName),
      homeTeamName,
      awayTeamName,
      homeTeamCode: toNationCode(homeTeamName),
      awayTeamCode: toNationCode(awayTeamName),
      homeScore: null,
      awayScore: null,
      date: currentDate,
      time: matchLine[2],
      venue: matchLine[5].trim(),
      leagueId: 'worldcup2026',
      competitionName: '2026 FIFA World Cup',
      teamType: 'nation',
      status: 'scheduled',
    });

    stages.at(-1)?.matchIds.push(id);
  }

  return { stages, matches };
}

async function fetchText(url: string) {
  const response = await fetch(url, { headers: { Accept: 'text/plain' } });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  return response.text();
}

async function main() {
  const worldCupModuleUrl = new URL('../src/data/worldCup.ts', import.meta.url);
  const { normalizeWorldCupTournament } = await import(worldCupModuleUrl.href);

  const [groupSource, finalsSource] = await Promise.all([
    fetchText(GROUPS_URL),
    fetchText(FINALS_URL),
  ]);

  const parsedGroups = parseGroupFile(groupSource);
  const parsedFinals = parseFinalsFile(finalsSource);

  const supportedSpotlights = Array.from(new Set(parsedGroups.groups.flatMap((group) => group.standings.map((row) => String(row.nationId)))))
    .filter((nationId) => SPOTLIGHTS[nationId])
    .map((nationId) => ({
      nationId,
      playerId: SPOTLIGHTS[nationId].playerId,
      note: SPOTLIGHTS[nationId].note,
    }));

  const tournament = {
    year: '2026',
    host: 'Canada · United States · Mexico',
    subtitle: 'Generated from openfootball/worldcup public fixture data.',
    matches: [...parsedGroups.matches, ...parsedFinals.matches],
    groups: parsedGroups.groups,
    stages: parsedFinals.stages,
    spotlights: supportedSpotlights,
  };

  const normalized = normalizeWorldCupTournament(tournament);
  if (!normalized) {
    throw new Error('Generated tournament payload failed validation.');
  }

  await writeFile(OUTPUT_PATH, `${JSON.stringify(normalized, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({
    output: OUTPUT_PATH.pathname,
    groups: normalized.groups.length,
    matches: normalized.matches.length,
    stages: normalized.stages.length,
    spotlights: normalized.spotlights.length,
  }, null, 2));
}

await main();
