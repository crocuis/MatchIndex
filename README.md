# MatchIndex

Football data exploration platform — high-density, desktop-first UI inspired by Football Manager's information architecture.

## Quick Start

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000)

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
