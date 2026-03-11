# Photo URL Sync Plan

## Scope

Implement only the agreed photo automation foundation:

1. Add DB structures to track external player photo sources and sync state.
2. Add TypeScript types and server-side DB helpers for those records.
3. Add a standalone sync script with `--dry-run` support.
4. Add env-driven provider configuration for API-Football, Wikimedia, and mirror base URL.
5. Verify with diagnostics, build, and script help/dry-run execution.

## Acceptance Criteria

1. `db/schema.sql` contains a new player photo source tracking table with indexes and clear status fields.
2. Server-side TypeScript can read and upsert photo source metadata without type errors.
3. A script in `scripts/` can run in `--help` and `--dry-run` modes without mutating data.
4. The script supports provider priority `api_football -> wikimedia`.
5. The script updates `players.photo_url` only outside dry-run mode.
6. Modified files have zero diagnostics errors.
7. `npm run build` succeeds.

## Minimal Implementation Steps

1. Extend schema with `player_photo_sources` table linked to `players` and `data_sources`.
   - Ensure `wikimedia` exists in `data_sources` seed data.
2. Add matching TypeScript interfaces in `src/data/types.ts` and SQL row mapping/helpers in `src/data/postgres.ts`.
3. Add a small shared automation config helper for environment variables.
4. Implement `scripts/sync-player-photos.mts`:
   - parse args
   - load candidate players
   - query providers in priority order
   - validate candidate URLs
   - log intended updates in dry-run
   - persist metadata and `players.photo_url` in non-dry-run mode
5. Add `package.json` scripts for help and dry-run execution.
6. Run diagnostics, build, and script verification.

## Verification

1. `npx tsc --noEmit` exits with code 0.
2. `npm run build` exits with code 0.
3. `node --experimental-strip-types scripts/sync-player-photos.mts --help` prints usage and exits successfully.
4. `node --experimental-strip-types scripts/sync-player-photos.mts --dry-run --limit=1` exits successfully and performs zero writes to `players.photo_url`.

## Risk Controls

1. No destructive writes in default mode.
2. No dependency additions.
3. Wikimedia matching uses strict enough fields to avoid obvious false positives.
4. Missing provider credentials degrade gracefully.

## Atomic Commit Strategy

1. Schema and types
2. DB helpers and config
3. Sync script and package scripts
4. Verification fixes only if needed
