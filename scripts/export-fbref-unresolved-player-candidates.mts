import { writeFile } from 'node:fs/promises';
import path from 'node:path';

interface CliOptions {
  competitionCodes: string[];
  outputPath: string;
  season: string;
}

function parseArgs(argv: string[]): CliOptions {
  const competitionArg = argv.find((arg) => arg.startsWith('--competitions='));
  const seasonArg = argv.find((arg) => arg.startsWith('--season='));
  const outputArg = argv.find((arg) => arg.startsWith('--output='));

  if (!competitionArg || !seasonArg) {
    throw new Error('--competitions and --season are required');
  }

  return {
    competitionCodes: competitionArg.slice('--competitions='.length).split(',').map((value) => value.trim().toUpperCase()).filter(Boolean),
    outputPath: outputArg?.slice('--output='.length) || 'data/fbref-unresolved-player-candidates.json',
    season: seasonArg.slice('--season='.length).trim(),
  };
}

async function main() {
  const envModuleUrl = new URL('./load-project-env.mts', import.meta.url);
  const { loadProjectEnv } = await import(envModuleUrl.href);
  loadProjectEnv();

  const options = parseArgs(process.argv.slice(2));
  const moduleUrl = new URL('../src/data/soccerdataFbrefMaterialize.ts', import.meta.url);
  const { listUnresolvedSoccerdataFbrefPlayers } = await import(moduleUrl.href);

  const results = [];
  for (const competitionCode of options.competitionCodes) {
    const candidates = await listUnresolvedSoccerdataFbrefPlayers({ competitionCode, season: options.season });
    results.push({ competitionCode, count: candidates.length, candidates });
  }

  const resolvedOutputPath = path.isAbsolute(options.outputPath)
    ? options.outputPath
    : path.join(process.cwd(), options.outputPath);
  await writeFile(resolvedOutputPath, `${JSON.stringify(results, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({ outputPath: resolvedOutputPath, competitions: options.competitionCodes, season: options.season }, null, 2));
}

await main();
