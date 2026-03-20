# DATA_RESTRUCTURE_PLAN.md

## 목적

MatchIndex의 데이터 구조를 아래 원칙에 맞춰 재정렬한다.

1. raw와 normalized를 분리
2. 매치 이벤트와 파생 지표를 분리
3. 공급원 ID와 내부 canonical ID를 분리
4. source event type과 canonical event type을 모두 보존
5. 좌표계와 시간 표현을 초기에 표준화
6. 종료 시즌과 진행 시즌의 운영 정책을 분리
7. 경기 단위 재처리를 기본 전제로 설계

## 현재 구조 요약

현재 MatchIndex는 이미 완전히 잘못된 구조는 아니다.

- `raw_payloads`, `source_sync_runs`, `source_sync_manifests`, `source_entity_mapping`가 있어 source-agnostic ingestion backbone은 존재한다.
- `player_contracts`, `player_season_stats`, `match_stats`, `match_lineups` 같은 normalized/derived read model도 이미 있다.
- `match_events` 계열 raw table은 제거했고, 대신 `match_event_artifacts` + `artifacts/` 기반 구조로 전환했다.

즉 완전 재설계보다, **현재 구조를 raw / normalized / derived / source mapping 관점으로 명시적으로 정리하는 것**이 맞다.

## Target Architecture

```text
source fetch
-> raw payload storage
-> source-specific parser / normalizer
-> canonical normalized artifact or table
-> derived match / season read model
-> application reads
```

## Layer 1. Raw Layer

### 역할

- source 원형 보존
- 재수집 없이 재처리 가능
- parser 버그 수정 후 재물질화 가능
- source 구조 변경 디버깅 가능

### 유지/확장 대상

- `raw_payloads`
- `source_sync_runs`
- `source_sync_manifests`
- source별 원형 artifact

### 원칙

- source 원형은 되도록 변형하지 않는다
- source별 payload 형식은 source 고유 구조를 유지한다
- match-level source 원형은 `artifacts/<source>/matches/.../raw-event-bundle.v1.json.gz` 같은 형태로 둔다

### 현재 코드베이스 대응

- `src/data/apiFootballCompetitionIngest.ts`
- `src/data/fbrefPlayerStatsIngest.ts`
- `src/data/soccerdataFbrefRawImport.ts`
- `src/data/sofascoreDetailsMaterialize.ts` 이전 raw payload 읽기 경로

## Layer 2. Normalized Layer

### 역할

- source별 차이를 canonical contract로 변환
- 앱이 직접 source 원형을 읽지 않게 함
- source와 상관없이 동일한 event / stat shape 제공

### 핵심 구조

#### 1. Canonical Event Artifact

현재 MatchIndex에서는 event table 대신 artifact-first가 더 적합하다.

- canonical normalized event는 테이블이 아니라 `analysis_detail.v2` artifact를 기준으로 본다
- source 원형과 분리된 contract를 갖는다

권장 필드:

- `sourceVendor`
- `sourceEventId`
- `sourceType`
- `canonicalType`
- `period`
- `minute`
- `second`
- `stoppageMinute`
- `matchSecond`
- `x`, `y`, `endX`, `endY`, `endZ` (0~100 표준화)
- `detail`
- `outcome`

현재 상태:

- `analysis_detail`는 이미 canonical event bundle에 가깝다
- 다만 `sourceType`, `period`, `matchSecond`, `stoppageMinute` 같은 필드는 source별로 아직 덜 일관적이다

#### 2. Event vs Derived Metric 분리

분리 대상:

- `analysis_detail` → 실제 사건
- `match_stats` → 팀 match aggregates
- `player_season_stats` → 시즌 집계
- 향후 필요 시:
  - `match_shot_metrics`
  - `match_player_metrics`
  - `match_team_metrics`

원칙:

- xG shot row는 event/shot artifact에 둔다
- xGChain, expected points, average rating 같은 파생 지표는 event artifact에 섞지 않는다

## Layer 3. Canonical Entity Mapping Layer

### 역할

- source별 player/team/match ID를 내부 canonical entity에 연결

### 유지 대상

- `source_entity_mapping`
- `players`
- `teams`
- `matches`
- `competitions`

### 원칙

- source ID와 canonical ID는 절대 섞지 않는다
- source adapter는 항상 `source_entity_mapping`을 통해 canonical entity에 연결한다
- `player_href`, external_id, team slug 등은 모두 `source_entity_mapping` 또는 source metadata로만 보존한다

### 현재 상태

- 이 원칙은 이미 대부분 맞게 구현돼 있다
- FBref mapping sync까지 추가되면서 구조적으로 더 강화되었다

## Layer 4. Derived Read Model Layer

### 역할

- 앱 조회 성능과 안정성을 위한 집계/스냅샷 테이블

### 유지 대상

- `match_stats`
- `match_lineups`
- `player_contracts`
- `team_seasons`
- `player_season_stats`
- `mv_top_scorers`

### 주의

- `player_season_stats`는 season-owner 정책이 필요하다
- `match_stats`는 source provenance를 추적할 수 있어야 한다
- `match_lineups`는 현재처럼 roster membership + minutes 중심으로 유지한다

## Source-aware Artifact Strategy

### 원칙

- raw artifact는 source별로 격리한다
- normalized layer에서만 통합한다

### 현재 경로 규칙

```text
artifacts/<source>/matches/<year>/<month>/<matchId>/analysis-detail.v1.json.gz
artifacts/<source>/matches/<year>/<month>/<matchId>/freeze-frames.v1.json.gz
artifacts/<source>/matches/<year>/<month>/<matchId>/visible-areas.v1.json.gz
```

### 의미

- source 원형 또는 source-normalized payload는 source별 폴더에 그대로 유지
- 앱은 reader에서 source priority를 적용해 canonical consumer contract로 읽음
- 추후 `normalized/` artifact layer를 추가하더라도 source raw를 훼손하지 않음

## Live vs Frozen Season Policy

### 원칙

- 종료 시즌과 진행 시즌은 ingestion/mutation 정책이 달라야 한다

### 권장 구조

- `competition_seasons` metadata에 owner / frozen 상태 보존
- 예시 metadata:

```json
{
  "owners": {
    "playerSeasonStats": "api_football",
    "matchArtifacts": "statsbomb"
  },
  "frozenAt": "2026-06-01T00:00:00Z"
}
```

### 운영 규칙

- live season: source pipeline이 지속 업데이트 가능
- frozen season: artifact/read model은 재처리 허용, 단 일반 sync는 overwrite 금지

세부 정책은 `SOURCE_OWNER_FREEZE_POLICY.md`를 따른다.

## Match-level Reprocessing

### 원칙

- “리그 전체 재수집”보다 “한 경기 재처리”를 기본 단위로 본다

### 필요한 것

- match 단위 ingestion status
- source revision/hash
- artifact regeneration hook
- derived table refresh hook

### 현재 상태

- `source_sync_runs`, `raw_payloads`, `match_event_artifacts`로 기반은 있다
- 아직 explicit `match_ingestion_status` 테이블은 없다

### 권장 방향

- 우선은 `source_sync_manifests` + `match_event_artifacts.updated_at` 조합으로 운영
- 필요해지면 `match_ingestion_status`를 추가

## 현재 구조와 충돌하는 지점

### 1. `DBMODEL.md`는 아직 `match_events` 중심 설명이 남아 있다

- 현재 실제 구조와 문서가 어긋난다
- artifact-first 구조로 문서 갱신 필요

### 2. `analysis_detail` contract가 source별로 완전히 동일하진 않다

- Understat / SofaScore / WhoScored / StatsBomb 간 field quality 차이 존재
- `analysis_detail.v2` 정의가 필요하다

### 3. `player_season_stats` owner 정책이 아직 명시적이지 않다

- API-Football, FBref, StatsBomb가 같은 시즌을 덮어쓸 수 있다
- source precedence / owner rule 문서화 필요

### 4. `match_stats` provenance 추적이 약하다

- source와 artifact version을 추적할 메타가 부족하다

## 권장 마이그레이션 순서

### Phase 1. 문서/계약 고정

1. `DBMODEL.md`를 artifact-first 구조로 갱신
2. `analysis_detail.v2` contract 문서화
3. source owner / frozen season 정책 문서화

### Phase 2. normalized contract 강화

1. `analysis_detail.v2` writer 추가
2. source별 adapter가 `sourceType`, `canonicalType`, `matchSecond`, normalized coords를 채우게 함
3. reader는 v2 우선 사용

세부 계약은 `ANALYSIS_DETAIL_V2_CONTRACT.md`를 따른다.

### Phase 3. provenance 강화

1. `match_stats` / `player_contracts` / `player_season_stats`에 provenance metadata 추가 검토
2. source overwrite rule 적용

### Phase 4. reprocessing 운영 단위 확립

1. match 단위 재처리 명령 추가
2. artifact regenerate + derived refresh hook 연결

## 후속 구현 우선순위

1. `DBMODEL.md`를 현재 artifact-first 구조에 맞게 재작성
2. `analysis_detail.v2` contract 설계
3. source owner matrix 문서화
4. `match_stats` provenance 보강
5. 필요 시 `match_ingestion_status` 추가

## 결론

현재 MatchIndex는 이미 제시된 7가지 원칙과 크게 어긋나지 않는다.

다만 핵심은:

- `match_events`를 다시 만들지 않는다
- `raw_payloads + source-aware raw artifacts`를 raw layer로 본다
- `analysis_detail.v2`를 canonical normalized event layer로 본다
- `match_stats`, `player_contracts`, `player_season_stats`를 derived layer로 유지한다
- source owner / frozen / reprocessing 정책을 문서와 metadata로 명시한다

즉, 다음 단계는 새 데이터 테이블을 많이 만드는 것보다 **현재 구조를 명확한 layer contract로 재정의하는 작업**이다.
