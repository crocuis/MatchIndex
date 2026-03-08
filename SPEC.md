# MatchIndex — Specification

## Overview

MatchIndex is a football information web service — a data-centric platform for exploring clubs, players, nations, leagues, and match results. Inspired by Football Manager 2024's information density, but with an entirely original UI design.

**Core Value**: Fast, scan-friendly exploration of football data. Not tactical analysis — pure data browsing.

---

## Design Direction

| Property | Value |
|----------|-------|
| Theme | Dark |
| Target | Desktop-first |
| Density | High (compact rows, dense panels) |
| Layout | Sidebar nav + panel/card/tab-based content |
| Feel | Scouting database / desktop application |
| Animations | Minimal — only functional transitions |
| Inspiration | FM24 information structure (not its visual design) |

### Design Principles

1. **Information Density** — maximize data per viewport, minimize whitespace
2. **Scan-friendly** — users should locate data within seconds
3. **Consistency** — every table, panel, and card follows the same visual language
4. **Desktop Application Feel** — not a typical web page; more like a native data tool
5. **No Decorative Elements** — every pixel serves a purpose

---

## Tech Stack

| Layer | Choice |
|-------|--------|
| Framework | Next.js (App Router) |
| Language | TypeScript |
| Styling | TailwindCSS |
| Data | Mock JSON (API-ready structure) |

---

## Routes

| Route | Page | Description |
|-------|------|-------------|
| `/` | Home Dashboard | Overview with featured matches, top leagues, quick stats |
| `/leagues/:id` | League Page | League details, standings, fixtures, top scorers |
| `/clubs/:id` | Club Page | Club profile, squad, matches, league position |
| `/players/:id` | Player Page | Player bio, stats, recent matches |
| `/nations/:id` | Nation Page | National team overview, players, matches |
| `/matches/:id` | Match Page | Match details, lineups, events, stats |
| `/results` | Results | Recent match results across leagues |
| `/search?q=` | Search Results | Global search across all entities |

---

## Page Specifications

### Home Dashboard (`/`)

- **Featured Matches** — today's/recent key matches with scores
- **League Standings Snapshot** — mini standings for top leagues
- **Recent Results** — compact results list
- **Top Scorers** — season leaders
- **Quick Navigation** — links to all leagues

### League Page (`/leagues/:id`)

| Section | Content |
|---------|---------|
| Summary | League name, country, season, number of clubs |
| Standings | Full table (Pos, Club, P, W, D, L, GF, GA, GD, Pts) |
| Recent Results | Last 10 matches in this league |
| Upcoming Fixtures | Next 10 scheduled matches |
| Top Scorers | Top 10 scorers with club, goals, assists |
| Clubs | Grid/list of all participating clubs |

### Club Page (`/clubs/:id`)

| Section | Content |
|---------|---------|
| Summary | Club name, badge placeholder, founded, stadium, league |
| Squad | Full player list (Name, Position, Age, Nationality, Apps, Goals) |
| Recent Matches | Last 10 matches with results |
| Upcoming Fixtures | Next 5 scheduled matches |
| League Position | Current standing with context (above/below teams) |
| Stats | Goals scored/conceded, win rate, form |

### Player Page (`/players/:id`)

| Section | Content |
|---------|---------|
| Bio | Name, age, date of birth, height |
| Details | Nationality, current club, position, shirt number |
| Season Stats | Appearances, goals, assists, minutes, cards |
| Recent Matches | Last 10 matches with performance |

### Nation Page (`/nations/:id`)

| Section | Content |
|---------|---------|
| Overview | Country name, flag placeholder, FIFA ranking, confederation |
| Squad | National team player list |
| Recent Matches | Last 10 international matches |

### Match Page (`/matches/:id`)

| Section | Content |
|---------|---------|
| Header | Home vs Away, score, date, competition |
| Venue | Stadium name, city |
| Lineups | Placeholder (home/away starting XI) |
| Events | Placeholder (goals, cards, substitutions timeline) |
| Stats | Placeholder (possession, shots, corners, fouls) |

### Results Page (`/results`)

- Filterable by league
- Chronological list of recent match results
- Compact table format (Date, Home, Score, Away, League)

### Search Page (`/search?q=`)

- Global search across players, clubs, leagues, nations
- Results grouped by entity type
- Instant-feeling (client-side filter on mock data)

---

## Data Models

### League

```typescript
interface League {
  id: string;
  name: string;
  country: string;
  season: string;
  logo?: string;
  numberOfClubs: number;
}
```

### Club

```typescript
interface Club {
  id: string;
  name: string;
  shortName: string;
  country: string;
  founded: number;
  stadium: string;
  stadiumCapacity: number;
  leagueId: string;
  logo?: string;
}
```

### Player

```typescript
interface Player {
  id: string;
  name: string;
  firstName: string;
  lastName: string;
  dateOfBirth: string;
  age: number;
  nationality: string;
  nationId: string;
  clubId: string;
  position: string;
  shirtNumber: number;
  height: number; // cm
  preferredFoot: 'Left' | 'Right' | 'Both';
  seasonStats: PlayerSeasonStats;
}

interface PlayerSeasonStats {
  appearances: number;
  goals: number;
  assists: number;
  minutesPlayed: number;
  yellowCards: number;
  redCards: number;
  cleanSheets?: number; // goalkeepers
}
```

### Nation

```typescript
interface Nation {
  id: string;
  name: string;
  code: string; // ISO 3166-1 alpha-3
  confederation: string;
  fifaRanking: number;
  flag?: string;
}
```

### Match

```typescript
interface Match {
  id: string;
  homeTeamId: string;
  awayTeamId: string;
  homeScore: number | null;
  awayScore: number | null;
  date: string;
  time: string;
  venue: string;
  leagueId: string;
  status: 'scheduled' | 'live' | 'finished';
  events?: MatchEvent[];
  stats?: MatchStats;
}

interface MatchEvent {
  minute: number;
  type: 'goal' | 'yellow_card' | 'red_card' | 'substitution';
  playerId: string;
  teamId: string;
  detail?: string;
}

interface MatchStats {
  possession: [number, number];
  shots: [number, number];
  shotsOnTarget: [number, number];
  corners: [number, number];
  fouls: [number, number];
}
```

### StandingRow

```typescript
interface StandingRow {
  position: number;
  clubId: string;
  played: number;
  won: number;
  drawn: number;
  lost: number;
  goalsFor: number;
  goalsAgainst: number;
  goalDifference: number;
  points: number;
  form: ('W' | 'D' | 'L')[];
}
```

### StatLeader

```typescript
interface StatLeader {
  playerId: string;
  clubId: string;
  leagueId: string;
  goals: number;
  assists: number;
}
```

---

## Component Architecture

### Layout Components

- `AppShell` — main layout wrapper (sidebar + content area)
- `Sidebar` — left navigation with league links, sections
- `TopBar` — global search input, breadcrumbs
- `PageHeader` — page title, metadata summary
- `TabGroup` — tab navigation within pages

### Data Display Components

- `DataTable` — reusable sortable table with compact rows
- `StandingsTable` — league standings (extends DataTable)
- `MatchCard` — compact match result display
- `FixtureCard` — upcoming match display
- `StatPanel` — key-value stat display panel
- `PlayerRow` — compact player info row
- `FormIndicator` — W/D/L form badges

### Utility Components

- `Badge` — small label (position, status, etc.)
- `EntityLink` — typed link to player/club/league/nation
- `SearchInput` — global search with debounce
- `EmptyState` — placeholder for empty sections
- `SectionCard` — titled card wrapper for page sections

---

## Folder Structure

```
src/
├── app/
│   ├── layout.tsx              # Root layout (AppShell)
│   ├── page.tsx                # Home dashboard
│   ├── leagues/
│   │   └── [id]/
│   │       └── page.tsx        # League page
│   ├── clubs/
│   │   └── [id]/
│   │       └── page.tsx        # Club page
│   ├── players/
│   │   └── [id]/
│   │       └── page.tsx        # Player page
│   ├── nations/
│   │   └── [id]/
│   │       └── page.tsx        # Nation page
│   ├── matches/
│   │   └── [id]/
│   │       └── page.tsx        # Match page
│   ├── results/
│   │   └── page.tsx            # Results page
│   └── search/
│       └── page.tsx            # Search page
├── components/
│   ├── layout/
│   │   ├── AppShell.tsx
│   │   ├── Sidebar.tsx
│   │   ├── TopBar.tsx
│   │   └── PageHeader.tsx
│   ├── data/
│   │   ├── DataTable.tsx
│   │   ├── StandingsTable.tsx
│   │   ├── MatchCard.tsx
│   │   ├── FixtureCard.tsx
│   │   └── StatPanel.tsx
│   └── ui/
│       ├── Badge.tsx
│       ├── EntityLink.tsx
│       ├── TabGroup.tsx
│       ├── SearchInput.tsx
│       ├── FormIndicator.tsx
│       ├── SectionCard.tsx
│       └── EmptyState.tsx
├── data/
│   ├── types.ts                # All TypeScript interfaces
│   ├── leagues.ts              # Mock league data
│   ├── clubs.ts                # Mock club data
│   ├── players.ts              # Mock player data
│   ├── nations.ts              # Mock nation data
│   ├── matches.ts              # Mock match data
│   ├── standings.ts            # Mock standings data
│   └── index.ts                # Data access functions
├── lib/
│   └── utils.ts                # Utility functions
└── styles/
    └── globals.css             # Global styles, Tailwind imports
```

---

## Mock Data Scope

For the initial implementation, include:

- **2 leagues** — Premier League, La Liga
- **8 clubs** (4 per league)
- **40 players** (5 per club)
- **4 nations** — England, Spain, France, Brazil
- **20 matches** (mix of finished and scheduled)
- **Full standings** for both leagues

This provides enough data to demonstrate all features without being overwhelming.

---

## Future API Integration

The data layer is designed for easy API migration:

1. All data access goes through functions in `data/index.ts`
2. These functions currently return mock data
3. To integrate a real API, replace function implementations
4. No component changes needed — they consume the same interfaces

Candidate APIs:
- [football-data.org](https://www.football-data.org/)
- [API-Football](https://www.api-football.com/)

---

## Definition of Done

- [ ] Runs locally with `npm run dev`
- [ ] All 8 routes render with mock data
- [ ] Page-to-page navigation works (links between entities)
- [ ] Data tables display correctly with compact layout
- [ ] Dark theme applied globally
- [ ] UI has high information density (FM-style)
- [ ] Global search filters across all entity types
- [ ] No TypeScript errors
- [ ] No console errors
