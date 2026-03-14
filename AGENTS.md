# AGENTS.md — MatchIndex

축구 데이터 탐색 플랫폼. Football Manager 스타일의 다크 테마, 데스크톱 퍼스트, 고밀도 UI.

## Stack

- **Next.js 16** (App Router, Turbopack) — React 19, `src/app/` 라우팅
- **TypeScript** (strict mode) — `@/` path alias → `./src/*`
- **TailwindCSS v4** — 커스텀 디자인 토큰, 다크 전용 테마
- **PostgreSQL** (`postgres` 라이브러리) — `db/schema.sql` DDL, `db/migrations/`
- **Redis** (ioredis) — 선택적 캐시 레이어 (`CACHE_ENABLED=true`)
- **next-intl** — i18n (en, ko), 쿠키 기반 로캘
- **Lucide React** — 아이콘 / **clsx + tailwind-merge** — `cn()` 유틸

## Commands

```bash
npm run dev              # 개발 서버 (Turbopack)
npm run build            # 프로덕션 빌드 — 빌드 에러 검증용
npm run lint             # ESLint (core-web-vitals + typescript)
npx tsc --noEmit         # 타입 체크만 (emit 없음)
```

테스트 프레임워크 없음. 테스트 러너, 테스트 파일 미존재.

## Language Policy

- 모든 문서, 에이전트 응답, 커밋 메시지는 **한국어**로 작성합니다.
- 코드 식별자(클래스, 메서드, 변수, API 이름)는 **영어**를 유지합니다.

## Architecture

```
src/
├── app/                    # App Router — 서버 컴포넌트 기본
│   ├── layout.tsx          # 루트 레이아웃 (폰트, i18n, Sidebar, TopBar)
│   ├── page.tsx            # 대시보드 (/)
│   ├── clubs/[id]/         # /clubs/:id (시즌 아카이브 포함)
│   ├── leagues/[id]/       # /leagues/:id
│   ├── players/[id]/       # /players/:id
│   ├── nations/[id]/       # /nations/:id
│   ├── matches/[id]/       # /matches/:id (분석 탭 포함)
│   ├── results/            # /results
│   ├── search/             # /search?q= (클라이언트 컴포넌트)
│   ├── worldcup/           # /worldcup
│   └── globals.css         # Tailwind 임포트 + 디자인 토큰
├── components/
│   ├── layout/             # Sidebar, TopBar, PageHeader
│   ├── data/               # DataTable, StandingsTable, MatchCard, FixtureCard,
│   │                       # FootballPitch, HeatMap, PassMap, ShotMap, MatchAnalysisTabs 등
│   └── ui/                 # Badge, EntityLink, TabGroup, SectionCard, ClubBadge,
│                           # NationFlag, PlayerAvatar, PaginationNav, DetailTabNav 등
├── data/
│   ├── types.ts            # 도메인 인터페이스 (League, Club, Player, Nation, Match 등)
│   ├── server.ts           # 서버 전용 DB 쿼리 re-export ('server-only' 가드)
│   ├── postgres.ts         # PostgreSQL 쿼리 구현 (전체 데이터 접근 레이어)
│   ├── index.ts            # 목 데이터 접근 레이어 (DB 미연결 시 폴백)
│   ├── api*.ts / statsbomb*.ts / footballData*.ts
│   │                       # 외부 API 인제스트/머티리얼라이즈 모듈
│   └── *.ts                # 목 데이터 파일 (clubs, players, matches 등)
├── config/
│   ├── app.ts              # APP_VERSION 등 앱 설정
│   └── nav.ts              # 사이드바 네비게이션 설정
├── i18n/
│   └── request.ts          # next-intl 로캘 설정
└── lib/
    ├── db.ts               # PostgreSQL 연결 풀 (postgres 라이브러리)
    ├── cache.ts            # Redis 읽기 캐시 (read-through, 계층형 TTL)
    ├── redis.ts            # Redis 클라이언트
    └── utils.ts            # cn(), formatDate(), formatNumber(), getPositionColor() 등
scripts/                    # 데이터 인제스트/동기화 CLI (.mts 파일들)
messages/                   # en.json, ko.json (i18n 번역)
db/
├── schema.sql              # PostgreSQL DDL (1300+ 줄)
└── migrations/             # SQL 마이그레이션 파일
```

## Data Layer

**이중 모드**: `DATABASE_URL` 설정 시 PostgreSQL (`src/data/server.ts` → `postgres.ts`), 미설정 시 목 데이터 폴백 (`src/data/index.ts`).

- 페이지에서는 `@/data/server` 에서 `*Db` 함수를 임포트 (예: `getClubByIdDb`)
- DB 함수는 대부분 `locale` 파라미터를 받아 한국어명 처리
- Redis 캐시는 `readThroughCache()` 패턴 사용 (`src/lib/cache.ts`)
- 스크립트는 `src/data/` 내 인제스트/머티리얼라이즈 모듈 + `scripts/` CLI 사용
- DB에 적재된 값이 단일 출처(single source of truth)다. 인제스트/머티리얼라이즈/조회/UI 어디에서도 DB 값을 임의 변환한 별도 정답 집합을 만들지 말고, 정규화·매핑·보정이 필요하면 적재 파이프라인 또는 스키마 레벨에서 해결한다

## Code Style

### Imports — 순서: 외부 → 내부 (`@/`), 상대경로 사용 금지

```typescript
import { notFound } from 'next/navigation';        // 1. Next.js / React
import type { Metadata } from 'next';
import { getTranslations } from 'next-intl/server'; // 2. 서드파티
import { PageHeader } from '@/components/layout/PageHeader';  // 3. 내부 컴포넌트
import { getClubByIdDb } from '@/data/server';      // 4. 데이터 / 유틸
import { cn } from '@/lib/utils';
```

- 항상 `@/` alias 사용, `./` `../` 금지
- 타입 전용 임포트에 `import type` 사용

### Naming

| 대상 | 규칙 | 예시 |
|---|---|---|
| 컴포넌트 파일 | PascalCase.tsx | `DataTable.tsx` |
| 유틸/설정 파일 | camelCase.ts | `utils.ts` |
| 컴포넌트 함수 | `export function PascalCase()` | `StandingsTable` |
| Props 인터페이스 | `{Component}Props` | `BadgeProps` |
| 도메인 모델 | `interface PascalCase` | `League`, `Club` |
| 유틸 함수 | camelCase | `formatDate()` |
| 상수 | UPPER_SNAKE_CASE | `NAV_GROUPS` |
| DB 쿼리 함수 | camelCase + `Db` 접미사 | `getClubByIdDb()` |

### TypeScript

- `interface` → 컴포넌트 props, 도메인 모델 / `type` → union, tuple, utility
- **strict mode** — `as any`, `@ts-ignore`, `@ts-expect-error` 사용 금지
- optional chaining (`?.`) + nullish coalescing (`??`) 활용
- 제네릭 활용 (예: `DataTable<T>`)

### Components

- **함수 선언문** 사용 — `export function Name()`, 화살표 함수 아님
- 컴포넌트는 **named export** — `export default`는 페이지만
- 서버 컴포넌트: `async function` (디렉티브 불필요)
- 클라이언트 컴포넌트: 첫 줄에 `'use client'`
- 모든 DOM 렌더링 컴포넌트에 `className` prop 제공

```typescript
// 서버 컴포넌트 (페이지)
export default async function ClubPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const club = await getClubByIdDb(id, locale);
  if (!club) notFound();
}

// 클라이언트 컴포넌트
'use client';
export function MatchCard({ match, className }: MatchCardProps) { }
```

### Styling — TailwindCSS v4 + 커스텀 토큰

| 토큰 | 용도 |
|---|---|
| `bg-surface-0` ~ `bg-surface-4` | 배경 레이어 (0=가장 어두움) |
| `text-text-primary/secondary/muted` | 텍스트 계층 |
| `border-border`, `border-border-subtle` | 테두리 |
| `text-accent-emerald` | 주 강조색 |
| `text-win/draw/loss` | 경기 결과 색상 |

- `cn()` 으로 조건부 클래스 병합: `cn('base-classes', condition && 'extra', className)`
- 폰트 크기: `text-[10px]` (라벨), `text-[11px]` (헤더), `text-[13px]` (본문) — FM 스타일 밀도

### i18n

- 서버: `const t = await getTranslations('namespace');`
- 클라이언트: `const t = useTranslations('namespace');`
- 번역 파일: `messages/en.json`, `messages/ko.json`
- 로캘 쿠키: `MATCHINDEX_LOCALE`, 폴백 `'en'`
- **엔티티명 로컬라이제이션**: 한국어 로캘 선택 시, DB에 저장된 한국어명(`koreanName` 등)이 존재하면 반드시 해당 값을 표시해야 한다 — 예) `getClubDisplayName(club, locale)` 패턴 참고

### Error Handling

- `notFound()` — 엔티티 미발견 시 사용 (from `next/navigation`)
- optional chaining + nullish coalescing 으로 안전한 접근
- 폴백 문자열: 예) `getClubName(id)` → 미발견 시 `'Unknown'` 반환

## ESLint

flat config (`eslint.config.mjs`): `eslint-config-next/core-web-vitals` + `typescript`. 커스텀 규칙 없음.

## Key Conventions

1. **DB 쿼리**: 페이지에서 `@/data/server`의 `*Db` 함수 사용 — 목 데이터 직접 임포트 금지
2. **내부 임포트**: 항상 `@/` alias — 상대경로 금지
3. **컴포넌트**: 함수 선언문 + named export
4. **Props**: 반드시 `{Name}Props` 인터페이스 정의
5. **스타일**: Tailwind 커스텀 토큰 + `cn()` — raw 색상값 금지
6. **페이지**: 서버 컴포넌트 (async) 기본, 인터랙션 필요 시만 클라이언트
7. **타입 안전성**: `as any` 금지, proper types 또는 `undefined` 반환
8. **디자인 밀도**: 의도적으로 높음 — 작은 폰트, 컴팩트 패딩
9. **엔티티명 표기 일관성**: 선수·구단·리그·국가 등의 이름은 축약어를 제외하고 동일 엔티티에 대해 항상 같은 표기를 사용 — DB 저장값(`name`, `koreanName` 등)을 단일 출처(single source of truth)로 취급하고, UI에서 임의로 다른 표기를 하드코딩하지 않는다
10. **적재 데이터 원칙**: DB에 적재되는 데이터 자체가 single source of truth다. 적재 이후 애플리케이션 계층에서 별도의 정답 테이블·하드코딩 매핑·임시 보정 로직으로 데이터 의미를 덮어쓰지 말고, 필요한 수정은 인제스트/머티리얼라이즈 또는 DB 레이어에서 반영한다
