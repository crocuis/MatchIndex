# SOURCE_OWNER_FREEZE_POLICY.md

## 목적

`competition_seasons` 단위로 source owner와 frozen season 정책을 명시해, `player_season_stats`, `player_contracts`, `match_stats`, `match artifacts`의 overwrite 규칙을 일관되게 관리한다.

## 배경

현재 MatchIndex는 여러 source를 동시에 쓴다.

- `api_football`
- `sofascore`
- `understat`
- `whoscored`
- `fbref`
- historical `statsbomb`

이 상태에서 owner 규칙이 없으면 마지막 writer가 이전 source 결과를 덮어쓸 수 있다.

특히 위험한 대상:

- `player_season_stats`
- `player_contracts`
- `match_stats`
- `match_event_artifacts`

## 기본 원칙

### 1. owner는 `competition_season` 단위로 둔다

- 시즌 read model의 owner는 경기 단위가 아니라 `competition_season` 단위로 정의한다.
- match artifact source는 match 단위로 다를 수 있지만, 기본 policy는 season owner를 우선한다.

### 2. raw는 항상 저장 가능, normalized/derived는 owner rule을 따른다

- `raw_payloads`: 항상 저장 가능
- source-aware raw artifact: 항상 저장 가능
- `player_season_stats`, `player_contracts`, `match_stats`: owner 또는 허용 source만 write 가능

### 3. frozen season은 일반 sync overwrite를 금지한다

- frozen된 시즌은 재처리는 가능하지만, routine sync는 write하면 안 된다.
- frozen season 변경은 explicit backfill/reprocess만 허용한다.

## 저장 위치

### 단기 권장안

`competition_seasons.source_metadata`에 아래 구조를 저장한다.

```json
{
  "owners": {
    "playerSeasonStats": "api_football",
    "playerContracts": "sofascore",
    "matchStats": "sofascore",
    "matchArtifacts": "understat+sofascore"
  },
  "preferredArtifactSources": [
    "understat",
    "whoscored",
    "sofascore"
  ],
  "frozenAt": null,
  "freezeReason": null,
  "backfillAllowedSources": [
    "fbref"
  ]
}
```

### 장기 대안

필요 시 `competition_season_source_policy` 같은 별도 테이블로 승격 가능하나, 현재는 `source_metadata`가 충분하다.

## 필드 정의

### `owners.playerSeasonStats`

- season aggregate stat의 주 source
- 예:
  - current/recent season: `api_football`
  - historical backfill: `fbref`

### `owners.playerContracts`

- season roster / player_contracts의 주 source
- 예:
  - recent season: `sofascore`
  - fbref backfill season: `fbref`

### `owners.matchStats`

- `match_stats` read model의 주 source
- 예:
  - `sofascore`
  - 일부 historical season: `statsbomb`

### `owners.matchArtifacts`

- canonical artifact owner
- 단일 문자열 또는 source strategy 문자열 허용

예:

- `statsbomb`
- `sofascore`
- `understat+sofascore`

### `preferredArtifactSources`

- reader가 source priority를 결정할 때 참고하는 우선순위
- 예:
  - shot map: `understat`
  - incidents/timeline: `sofascore`
  - rich event: `whoscored`

### `frozenAt`

- null이면 live/open season
- timestamp가 있으면 frozen season

### `backfillAllowedSources`

- owner는 아니지만 빈 값만 보강할 수 있는 source 목록

예:

- `fbref`

## 운영 규칙

### Rule 1. Live season

- owner source는 overwrite 가능
- non-owner source는 raw만 저장
- backfillAllowed source는 missing row만 insert 가능

### Rule 2. Frozen season

- 일반 sync는 derived layer write 금지
- explicit reprocess/backfill만 허용
- raw layer는 필요 시 적재 가능

### Rule 3. Artifact source selection

- artifact 파일은 source별로 모두 저장 가능
- 앱 reader는 `preferredArtifactSources`와 source availability를 보고 선택

즉 source raw는 격리 저장, consumer는 normalized selection policy로 통합한다.

## 추천 owner matrix (초안)

### Current / recent seasons

```json
{
  "playerSeasonStats": "api_football",
  "playerContracts": "sofascore",
  "matchStats": "sofascore",
  "matchArtifacts": "understat+sofascore"
}
```

### Historical seasons with FBref backfill

```json
{
  "playerSeasonStats": "fbref",
  "playerContracts": "fbref",
  "matchStats": "sofascore",
  "matchArtifacts": "statsbomb"
}
```

### StatsBomb open-data historical seasons

```json
{
  "playerSeasonStats": "fbref",
  "playerContracts": "fbref",
  "matchStats": "statsbomb",
  "matchArtifacts": "statsbomb"
}
```

## 적용 순서

### Phase 1

- 문서 확정
- `competition_seasons.source_metadata`에 owner/frozen 구조 저장 시작

### Phase 2

- source materializer에서 write 전에 owner/frozen 검사 추가

### Phase 3

- artifact reader가 `preferredArtifactSources`를 참고하도록 확장

### Phase 4

- 필요 시 admin tooling 또는 season policy sync script 추가

## 현재 코드 기준 영향 지점

- `src/data/apiFootballPlayerStatsMaterialize.ts`
- `src/data/soccerdataFbrefMaterialize.ts`
- `src/data/soccerdataFbrefContractsBackfill.ts`
- `src/data/sofascoreDetailsMaterialize.ts`
- `src/data/statsbombMaterializeDetails.ts`
- `src/data/postgres.ts`
- `src/data/apiFootballCompetitionMaterialize.ts`
- `src/data/footballDataOrgMaterialize.ts`

## 비목표

- 지금 당장 모든 materializer에 owner enforcement를 다 넣는 것
- source별 모든 예외를 초기 문서에서 완벽히 규정하는 것

## 결론

owner / frozen 정책은 새 테이블보다 먼저 **문서 + `competition_seasons.source_metadata` 계약**으로 시작하는 게 가장 안전하다.

핵심은:

- source raw는 계속 저장
- derived/read model overwrite만 정책으로 제한
- frozen season은 일반 sync 차단
- artifact는 source별로 격리 저장, reader가 선택해서 통합
