# AGENTS.md — MatchIndex

Football data exploration platform. Dark-themed, desktop-first, high-density UI inspired by Football Manager.

## Stack

- **Next.js 16** (App Router, Turbopack) — React 19, `src/app/` routing
- **TypeScript** (strict mode) — `@/` path alias → `./src/*`
- **TailwindCSS v4** — custom design tokens, dark-only theme
- **next-intl** — i18n (en, ko), cookie-based locale
- **Lucide React** — icons
- **clsx + tailwind-merge** — `cn()` class utility

## Commands

```bash
npm run dev          # Start dev server (Turbopack)
npm run build        # Production build — use to verify no build errors
npm run start        # Start production server
npm run lint         # ESLint (eslint-config-next with core-web-vitals + typescript)
npx tsc --noEmit     # Type-check without emitting
```

## Language Policy

- 모든 문서, 에이전트 응답, 커밋 메시지는 **한국어**로 작성합니다.
- 코드 식별자(클래스, 메서드, 변수, API 이름)는 기존 코드 관례에 따라 **영어**를 유지합니다.

No test framework is configured. No test runner, no test files exist.

## Architecture

```
src/
├── app/                    # Next.js App Router — pages are server components by default
│   ├── layout.tsx          # Root layout (fonts, NextIntlClientProvider, Sidebar, TopBar)
│   ├── page.tsx            # Dashboard (/)
│   ├── clubs/[id]/         # /clubs/:id
│   ├── leagues/[id]/       # /leagues/:id
│   ├── players/[id]/       # /players/:id
│   ├── nations/[id]/       # /nations/:id
│   ├── matches/[id]/       # /matches/:id
│   ├── results/            # /results
│   ├── search/             # /search?q= (client component)
│   └── globals.css         # Tailwind imports + design tokens
├── components/
│   ├── layout/             # AppShell pieces: Sidebar, TopBar, PageHeader
│   ├── data/               # Data display: DataTable, StandingsTable, MatchCard, StatPanel
│   └── ui/                 # Primitives: Badge, EntityLink, TabGroup, SectionCard, ClubBadge
├── data/
│   ├── types.ts            # Domain interfaces (League, Club, Player, Nation, Match, etc.)
│   ├── index.ts            # Data access layer — ALL queries go through here
│   ├── leagues.ts          # Mock data
│   ├── clubs.ts            # Mock data
│   ├── players.ts          # Mock data
│   ├── nations.ts          # Mock data
│   ├── matches.ts          # Mock data
│   └── standings.ts        # Mock data
├── config/
│   └── nav.ts              # Sidebar navigation config (typed NavItem/NavGroup)
├── i18n/
│   └── request.ts          # next-intl locale config (cookie-based, fallback to 'en')
├── lib/
│   └── utils.ts            # cn(), formatDate(), formatNumber(), getPositionColor(), etc.
└── styles/                 # (globals.css lives in app/ instead)
messages/
├── en.json                 # English translations
└── ko.json                 # Korean translations
db/
└── schema.sql              # PostgreSQL DDL (future — not connected yet)
```

## Data Layer

All data access goes through `src/data/index.ts`. Currently mock data via in-memory Maps.
Designed for future API swap — change implementation in `index.ts`, consumers stay unchanged.

```typescript
// Pattern: Map-based O(1) lookup, returns T | undefined
export function getClubById(id: string): Club | undefined {
  return clubMap.get(id);
}
```

- No API routes exist (`src/app/api/` does not exist)
- No server actions
- No ORM — `db/schema.sql` is a future PostgreSQL schema (not connected)

## Code Style

### Imports

Order: **external → internal (via `@/`)** — never use relative paths.

```typescript
// 1. Next.js / React
import { notFound } from 'next/navigation';
import type { Metadata } from 'next';
// 2. Third-party
import { getTranslations } from 'next-intl/server';
// 3. Internal components
import { PageHeader } from '@/components/layout/PageHeader';
import { SectionCard } from '@/components/ui/SectionCard';
// 4. Data / utilities
import { getClubById, getPlayersByClub } from '@/data';
import { cn, formatNumber } from '@/lib/utils';
```

- Always use `@/` alias, never `./` or `../`
- Use `import type` for type-only imports
- Group named imports from same module on one line

### Naming

| What | Convention | Example |
|---|---|---|
| Component files | PascalCase.tsx | `DataTable.tsx`, `MatchCard.tsx` |
| Utility/config files | camelCase.ts | `utils.ts`, `nav.ts` |
| Component functions | PascalCase | `export function StandingsTable()` |
| Utility functions | camelCase | `formatDate()`, `getPositionColor()` |
| Props interfaces | `{Component}Props` | `BadgeProps`, `DataTableProps` |
| Constants | UPPER_SNAKE_CASE | `NAV_GROUPS`, `LOCALE_COOKIE` |
| Domain interfaces | PascalCase | `League`, `Club`, `Player` |
| Route params | `[id]` dirs | `clubs/[id]/page.tsx` |

### TypeScript

- **`interface`** for component props and domain models
- **`type`** for unions, tuples, utility types
- **Strict mode** enabled — do not use `as any`, `@ts-ignore`, or `@ts-expect-error`
- Optional chaining (`?.`) and nullish coalescing (`??`) for safe access
- Generics for reusable components (see `DataTable<T>`)

```typescript
// Props: always interface, destructured with defaults
interface StandingsTableProps {
  standings: StandingRow[];
  compact?: boolean;
  className?: string;
}

export function StandingsTable({ standings, compact = false, className }: StandingsTableProps) {}

// Union types
export type EntityType = 'player' | 'club' | 'league' | 'nation';
```

### Components

- **Function declarations** — `export function Name()`, not arrow functions
- **No default exports** for components — only pages use `export default`
- Server components are `async` functions (default, no directive needed)
- Client components have `'use client'` as the very first line

```typescript
// Server component (page) — async, data fetching at top level
export default async function ClubPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const club = getClubById(id);
  if (!club) notFound();
  // ...
}

// Client component — 'use client' directive, hooks allowed
'use client';
export function MatchCard({ match, className }: MatchCardProps) {
  const router = useRouter();
  const t = useTranslations('matchStatus');
  // ...
}
```

### Dynamic Route Pages

Pages with `[id]` params implement `generateStaticParams` and `generateMetadata`:

```typescript
export async function generateStaticParams() {
  return getClubs().map((c) => ({ id: c.id }));
}

export async function generateMetadata({ params }: { params: Promise<{ id: string }> }): Promise<Metadata> {
  const { id } = await params;
  const club = getClubById(id);
  return { title: club?.name ?? 'Club' };
}
```

### Styling

TailwindCSS v4 with custom design tokens defined in `src/app/globals.css`.

**Custom tokens** (use these, not raw colors):

| Token | Usage |
|---|---|
| `bg-surface-0` through `bg-surface-4` | Background layers (0=darkest) |
| `text-text-primary/secondary/muted` | Text hierarchy |
| `border-border`, `border-border-subtle` | Borders |
| `text-accent-emerald` | Primary accent |
| `text-win/draw/loss` | Match result colors |

**Class merging** — always use `cn()` for conditional classes:

```typescript
import { cn } from '@/lib/utils';

<div className={cn(
  'rounded-lg border border-border bg-surface-1',  // base
  compact && 'px-2',                                 // conditional
  className                                          // passthrough
)}>
```

**Font sizes**: `text-[10px]` (labels), `text-[11px]` (headers), `text-[13px]` (body) — explicit pixel values for FM-style density.

### Internationalization (i18n)

- **Server components**: `const t = await getTranslations('namespace');`
- **Client components**: `const t = useTranslations('namespace');`
- Translation files: `messages/en.json`, `messages/ko.json`
- Locale stored in cookie `MATCHINDEX_LOCALE`, fallback `'en'`
- Namespaced: `dashboard`, `nav`, `common`, `standings`, `table`, etc.

```typescript
// Server
const tDashboard = await getTranslations('dashboard');
<PageHeader title={tDashboard('title')} />

// Client
const t = useTranslations('matchStatus');
<span>{t(match.status)}</span>
```

### Error Handling

- `notFound()` from `next/navigation` for missing entities in pages
- Optional chaining + nullish coalescing for safe data access
- Fallback strings: `getClubName(id)` returns `'Unknown'` if not found
- No try/catch blocks in current codebase (mock data never throws)

### State Management

React hooks only — no external state library:
- `useState` for local UI state
- `useTransition` for async operations (locale switching)
- `useRouter` / `usePathname` for navigation
- `useSearchParams` for query params (search page)

## ESLint

Flat config (`eslint.config.mjs`): `eslint-config-next/core-web-vitals` + `eslint-config-next/typescript`.
No custom rules added. Run `npm run lint` to check.

## Key Conventions Summary

1. **All data queries** go through `src/data/index.ts` — never import mock data directly
2. **All internal imports** use `@/` alias — never relative paths
3. **Components** use function declarations with named exports
4. **Props** always get a `{Name}Props` interface
5. **Styling** uses Tailwind with custom tokens — use `cn()` for conditionals
6. **Pages** are server components (async) unless they need interactivity
7. **Client components** get `'use client'` and use `useTranslations()` for i18n
8. **No `as any`** — use proper types or `undefined` returns
9. **`className` prop** on all components that render DOM elements
10. **Design density** is intentionally high — small font sizes, compact padding
