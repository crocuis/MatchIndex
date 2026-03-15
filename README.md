# MatchIndex

Football data exploration platform — high-density, desktop-first UI inspired by Football Manager's information architecture.

## Quick Start

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000)

## Scheduled Fixture Sync

```bash
npm run fixtures:sync-api-football:dry-run
npm run fixtures:sync-football-data:dry-run
npm run fixtures:sync-api-football
npm run fixtures:sync-football-data
npm run scheduled:cron -- --print
```

- Daily fixture sync targets `PL`, `PD`, `SA`, `BL1`, `FL1`, `CL`, `EL`
- The sync runs ingest + materialize for the previous/current European season window
- Install the cron entry with `npm run scheduled:cron -- --install --jobs=api-football-fixtures-daily`
- `football-data.org` sync fetches the full selected-season match set so scheduled fixtures are included without a status-specific dependency
- Install the football-data.org daily job with `npm run scheduled:cron -- --install --jobs=football-data-fixtures-daily`

## Player Contracts Backfill

- 시즌별 선수단 복구용 실행 순서는 `PLAYER_CONTRACTS_BACKFILL_RUNBOOK.md`를 참고
- 기본 권장 순서: API-Football `player-stats:materialize-api-football` -> Sofascore `soccerdata:materialize-sofascore-details` -> 필요 시 StatsBomb `statsbomb:materialize-details`

## FBref Source Plan

- `football-data-webscraping` 도입 범위와 FBref source adapter 방향은 `FBREF_SOURCE_ADOPTION_PLAN.md`를 참고
- 현재 결론은 저장소 전체 도입이 아니라 `FBref scraping reference`만 선택적으로 차용하는 것이다
- ingest scaffold dry-run: `npm run player-stats:ingest-fbref -- --competition=PL --season=2024-2025`
- write mode는 FBref HTML 원문을 `raw_payloads`와 `source_sync_manifests`에 저장한다
- historical/backfill 실행 순서는 `FBREF_HISTORICAL_BACKFILL_RUNBOOK.md`를 참고
- WSL/Windows 비교 진단과 cookie-file 재실행 절차도 `FBREF_HISTORICAL_BACKFILL_RUNBOOK.md`에 정리되어 있다

## Match Event Artifacts

- artifact 운영/runbook은 `MATCH_EVENT_ARTIFACTS_RUNBOOK.md`를 참고
- artifact는 source pipeline(StatsBomb, SofaScore, API-Football 등)이 직접 생성한다
- artifact 경로는 `artifacts/<source>/matches/<year>/<month>/<matchId>/...` 구조를 사용한다
- artifact 기본 경로는 `artifacts/`이며 `MATCH_EVENT_ARTIFACTS_DIR`로 override 가능
- 앱 read 경로 기본값은 `jsDelivr` CDN이며 `MATCH_EVENT_ARTIFACT_REMOTE_BASE_URL`, `MATCH_EVENT_ARTIFACT_READ_MODE`로 제어 가능
- private GitHub 저장소를 쓰면 `MATCH_EVENT_ARTIFACT_GITHUB_TOKEN`과 `MATCH_EVENT_ARTIFACT_GITHUB_OWNER/REPO/REF`를 함께 설정하면 된다

## Stack

- **Next.js 16** (App Router, Turbopack)
- **TypeScript**
- **TailwindCSS v4** (dark theme, custom design tokens)
- **Lucide React** (icons)

## Routes

| Route | Description |
|---|---|
| `/` | Dashboard — league standings, recent results, top scorers |
| `/leagues/:id` | League — standings, fixtures, clubs, top scorers |
| `/clubs/:id` | Club — squad list, matches, league position |
| `/players/:id` | Player — bio, season stats, recent matches |
| `/nations/:id` | Nation — FIFA ranking, national team players |
| `/matches/:id` | Match — score, events, statistics |
| `/results` | All results — filterable by league |
| `/search?q=` | Global search — players, clubs, leagues, nations |

## Architecture

```
src/
├── app/                    # Next.js App Router pages
├── components/
│   ├── layout/             # Sidebar, TopBar, PageHeader
│   ├── data/               # DataTable, StandingsTable, MatchCard, etc.
│   └── ui/                 # Badge, EntityLink, TabGroup, etc.
├── data/                   # Types + mock data + data access layer
├── config/                 # Navigation config
├── lib/                    # Utilities (cn, formatters)
└── styles/                 # Global CSS + design tokens
```

### Data Layer

All data access goes through `src/data/index.ts`. Currently returns mock data — swap implementations to connect a real API without changing any components.

```typescript
// Current: mock data
export function getClubById(id: string): Club | undefined {
  return clubMap.get(id);
}

// Future: API call
export async function getClubById(id: string): Promise<Club | undefined> {
  const res = await fetch(`/api/clubs/${id}`);
  return res.json();
}
```

### Mock Data Scope

- 2 leagues (Premier League, La Liga)
- 8 clubs (4 per league)
- 40 players (5 per club)
- 4 nations (England, Spain, France, Brazil)
- 20 matches (10 finished, 10 scheduled)
- Full standings for both leagues

## Design

- Dark theme with custom color tokens (surface layers, text hierarchy)
- 13px base font, 11px table headers — FM-style density
- Tabular numbers for all stats columns
- Custom scrollbar styling
- Panel/card-based page sections

## API Integration TODO

- [ ] Replace mock data functions with API calls in `src/data/index.ts`
- [ ] Add loading states (Suspense boundaries)
- [ ] Add error boundaries
- [ ] Implement real-time match updates (WebSocket or polling)
- [ ] Add player images / club logos
- [ ] Add lineup data for match pages
- [ ] Add match events timeline
- [ ] Integrate with [football-data.org](https://www.football-data.org/) or [API-Football](https://www.api-football.com/)
- [ ] Add pagination for large datasets
- [ ] Add sorting to all data tables
- [ ] Add more leagues and clubs
