import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';

function stripWrappingQuotes(value: string) {
  if (
    (value.startsWith('"') && value.endsWith('"'))
    || (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  return value;
}

function parseEnvFile(contents: string) {
  const parsed: Record<string, string> = {};

  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) {
      continue;
    }

    const separatorIndex = line.indexOf('=');
    if (separatorIndex <= 0) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    const value = stripWrappingQuotes(line.slice(separatorIndex + 1).trim());

    if (!key) {
      continue;
    }

    parsed[key] = value;
  }

  return parsed;
}

function isMissingEnvValue(value: string | undefined) {
  return value === undefined || value.trim() === '';
}

export function loadProjectEnv() {
  const root = process.cwd();
  const candidates = [
    '.env',
    '.env.local',
    '.env.development',
    '.env.development.local',
  ];

  for (const candidate of candidates) {
    const filePath = path.join(root, candidate);
    if (!existsSync(filePath)) {
      continue;
    }

    const parsed = parseEnvFile(readFileSync(filePath, 'utf8'));
    for (const [key, value] of Object.entries(parsed)) {
      if (isMissingEnvValue(process.env[key])) {
        process.env[key] = value;
      }
    }
  }
}
