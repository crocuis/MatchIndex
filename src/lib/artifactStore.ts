import 'server-only';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { promisify } from 'node:util';
import { gunzip, gzip } from 'node:zlib';

const gunzipAsync = promisify(gunzip);
const gzipAsync = promisify(gzip);

type ArtifactReadMode = 'local-first' | 'remote-first' | 'remote-only';

const DEFAULT_ARTIFACT_REMOTE_BASE_URL = 'https://cdn.jsdelivr.net/gh/crocuis/football_data@main';
const DEFAULT_ARTIFACT_GITHUB_OWNER = 'crocuis';
const DEFAULT_ARTIFACT_GITHUB_REPO = 'football_data';
const DEFAULT_ARTIFACT_GITHUB_REF = 'main';

function normalizeArtifactSourceVendor(sourceVendor: string | null | undefined) {
  const normalized = (sourceVendor?.trim().toLowerCase() || 'unknown').replace(/[^a-z0-9_-]+/g, '-');

  if (normalized === 'unknown') {
    return 'statsbomb';
  }

  if (normalized === 'soccerdata_fbref') {
    return 'fbref';
  }

  return normalized;
}

function getArtifactRootDirectory() {
  const configuredPath = process.env.MATCH_EVENT_ARTIFACTS_DIR?.trim();

  if (!configuredPath) {
    return path.join(process.cwd(), 'artifacts');
  }

  return path.isAbsolute(configuredPath)
    ? configuredPath
    : path.join(process.cwd(), configuredPath);
}

function getArtifactRemoteBaseUrl() {
  return process.env.MATCH_EVENT_ARTIFACT_REMOTE_BASE_URL?.trim() || DEFAULT_ARTIFACT_REMOTE_BASE_URL;
}

function getArtifactGitHubOwner() {
  return process.env.MATCH_EVENT_ARTIFACT_GITHUB_OWNER?.trim() || DEFAULT_ARTIFACT_GITHUB_OWNER;
}

function getArtifactGitHubRepo() {
  return process.env.MATCH_EVENT_ARTIFACT_GITHUB_REPO?.trim() || DEFAULT_ARTIFACT_GITHUB_REPO;
}

function getArtifactGitHubRef() {
  return process.env.MATCH_EVENT_ARTIFACT_GITHUB_REF?.trim() || DEFAULT_ARTIFACT_GITHUB_REF;
}

function getArtifactGitHubToken() {
  return process.env.MATCH_EVENT_ARTIFACT_GITHUB_TOKEN?.trim() || process.env.GITHUB_TOKEN?.trim() || '';
}

function getArtifactReadMode(): ArtifactReadMode {
  const configuredMode = process.env.MATCH_EVENT_ARTIFACT_READ_MODE?.trim();

  if (configuredMode === 'local-first' || configuredMode === 'remote-first' || configuredMode === 'remote-only') {
    return configuredMode;
  }

  return 'remote-first';
}

export function resolveArtifactStoragePath(storageKey: string) {
  return path.isAbsolute(storageKey)
    ? storageKey
    : path.join(getArtifactRootDirectory(), storageKey);
}

export function resolveArtifactRemoteUrl(storageKey: string) {
  if (/^https?:\/\//.test(storageKey)) {
    return storageKey;
  }

  const normalizedStorageKey = storageKey.replace(/^\/+/, '').split(path.sep).join('/');
  return `${getArtifactRemoteBaseUrl().replace(/\/$/, '')}/${normalizedStorageKey}`;
}

function resolveArtifactGitHubContentsUrl(storageKey: string) {
  const normalizedStorageKey = storageKey.replace(/^\/+/, '').split(path.sep).join('/');
  const encodedSegments = normalizedStorageKey.split('/').map((segment) => encodeURIComponent(segment));
  return `https://api.github.com/repos/${getArtifactGitHubOwner()}/${getArtifactGitHubRepo()}/contents/${encodedSegments.join('/')}?ref=${encodeURIComponent(getArtifactGitHubRef())}`;
}

export function buildMatchArtifactStorageKey(matchDate: string, matchId: string | number, fileName: string) {
  const [year = 'unknown', month = 'unknown'] = matchDate.split('-');
  return path.join('matches', year, month, String(matchId), fileName);
}

export function buildSourceAwareMatchArtifactStorageKey(
  sourceVendor: string | null | undefined,
  matchDate: string,
  matchId: string | number,
  fileName: string,
) {
  const [year = 'unknown', month = 'unknown'] = matchDate.split('-');
  const sourceSegment = normalizeArtifactSourceVendor(sourceVendor);
  return path.join(sourceSegment, 'matches', year, month, String(matchId), fileName);
}

export async function serializeJsonGzipArtifact(payload: unknown) {
  return gzipAsync(JSON.stringify(payload));
}

export function computeArtifactChecksumSha256(contents: Uint8Array) {
  return createHash('sha256').update(contents).digest('hex');
}

export async function readJsonGzipArtifact<T>(storageKey: string): Promise<T> {
  const compressed = await readArtifactContents(storageKey);
  const buffer = await gunzipAsync(compressed);

  return JSON.parse(buffer.toString('utf8')) as T;
}

async function readArtifactContents(storageKey: string) {
  const readMode = getArtifactReadMode();

  if (readMode === 'local-first') {
    try {
      return await readLocalArtifactContents(storageKey);
    } catch {
      return readRemoteArtifactContents(storageKey);
    }
  }

  if (readMode === 'remote-only') {
    return readRemoteArtifactContents(storageKey);
  }

  try {
    return await readRemoteArtifactContents(storageKey);
  } catch {
    return readLocalArtifactContents(storageKey);
  }
}

async function readLocalArtifactContents(storageKey: string) {
  const filePath = resolveArtifactStoragePath(storageKey);
  return readFile(filePath);
}

async function readRemoteArtifactContents(storageKey: string) {
  const githubToken = getArtifactGitHubToken();

  if (githubToken) {
    return readArtifactContentsFromGitHub(storageKey, githubToken);
  }

  const url = resolveArtifactRemoteUrl(storageKey);
  const response = await fetch(url, {
    headers: {
      Accept: 'application/octet-stream',
    },
    next: { revalidate: 60 * 60 },
  });

  if (!response.ok) {
    throw new Error(`Failed to load artifact: ${url} (${response.status})`);
  }

  return Buffer.from(await response.arrayBuffer());
}

async function readArtifactContentsFromGitHub(storageKey: string, githubToken: string) {
  const url = resolveArtifactGitHubContentsUrl(storageKey);
  const response = await fetch(url, {
    headers: {
      Accept: 'application/vnd.github.raw',
      Authorization: `Bearer ${githubToken}`,
      'X-GitHub-Api-Version': '2022-11-28',
    },
    next: { revalidate: 60 * 60 },
  });

  if (!response.ok) {
    throw new Error(`Failed to load artifact from GitHub: ${url} (${response.status})`);
  }

  return Buffer.from(await response.arrayBuffer());
}

export async function writeJsonGzipArtifact(storageKey: string, payload: unknown) {
  const filePath = resolveArtifactStoragePath(storageKey);
  const compressed = await serializeJsonGzipArtifact(payload);
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, compressed);

  return {
    filePath,
    byteSize: compressed.byteLength,
    checksumSha256: computeArtifactChecksumSha256(compressed),
  };
}
