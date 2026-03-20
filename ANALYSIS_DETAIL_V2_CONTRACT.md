# ANALYSIS_DETAIL_V2_CONTRACT.md

## 목적

`analysis_detail` artifact를 source별 느슨한 event bundle에서, MatchIndex의 canonical normalized event contract로 올리기 위한 v2 규약을 정의한다.

## 배경

현재 v1은 이미 artifact-first 구조를 지원하지만 source별 필드 품질이 다르다.

- StatsBomb: 비교적 풍부한 좌표/시간/이벤트 정보
- SofaScore: incidents 중심, 좌표 부족
- Understat: shot/xG 중심
- WhoScored: event stream 풍부하지만 source-specific 필드가 많음
- API-Football: incidents 중심, 좌표 거의 없음

즉 v1은 "사용 가능"하지만, source별 편차가 커서 canonical normalized layer로 보기엔 아직 느슨하다.

## v1의 한계

현재 `src/data/types.ts:476` 기준 `MatchAnalysisArtifactEvent`는 아래 필드만 보장한다.

- `sourceEventId`
- `eventIndex`
- `minute`
- `second`
- `type`
- `teamId`
- `playerId`
- `secondaryPlayerId`
- `locationX/Y`
- `endLocationX/Y/Z`
- `underPressure`
- `statsbombXg`
- `detail`
- `outcome`

부족한 점:

- source 원본 type/subtype 보존이 약함
- canonical type과 source type 구분이 없음
- `period`, `stoppageMinute`, `matchSecond`가 없음
- source raw 좌표와 normalized 좌표 구분이 없음
- event와 derived metric의 provenance가 약함

## v2 목표

v2는 다음을 만족해야 한다.

1. source 원형과 normalized meaning을 구분한다
2. 앱은 source와 무관하게 동일한 event contract를 읽는다
3. 시간과 좌표는 가능한 범위에서 표준화한다
4. 파생 지표는 event payload에 최소한만 두고 provenance를 남긴다

## Top-level Payload Contract

```ts
interface MatchAnalysisArtifactPayloadV2 {
  version: 2;
  matchId: number;
  artifactType: 'analysis_detail';
  sourceVendor: string;
  generatedAt: string;
  sourceRevision?: string | null;
  coordinateSystem: 'pitch-120x80' | 'pitch-100x100' | 'pitch-0to1';
  normalizedCoordinateSystem: 'pitch-100x100';
  events: MatchAnalysisArtifactEventV2[];
}
```

## Event Contract

```ts
interface MatchAnalysisArtifactEventV2 {
  sourceEventId: string | null;
  sourceType: string | null;
  sourceSubtype: string | null;
  canonicalType: MatchAnalysisEventType;

  eventIndex: number;
  period: number | null;
  minute: number;
  second: number | null;
  stoppageMinute: number | null;
  matchSecond: number | null;

  teamId: number;
  playerId: number | null;
  secondaryPlayerId: number | null;

  sourceLocationX: number | null;
  sourceLocationY: number | null;
  sourceEndLocationX: number | null;
  sourceEndLocationY: number | null;
  sourceEndLocationZ: number | null;

  locationX: number | null;
  locationY: number | null;
  endLocationX: number | null;
  endLocationY: number | null;
  endLocationZ: number | null;

  underPressure: boolean | null;
  detail: string | null;
  outcome: string | null;

  metrics?: {
    xg?: number | null;
    psxg?: number | null;
    xgChain?: number | null;
  };

  sourcePayload?: Record<string, unknown> | null;
}
```

## 필드 설명

### source vs canonical

- `sourceType`: 공급원 원본 event type
- `sourceSubtype`: 공급원 세부 type / qualifier / result
- `canonicalType`: MatchIndex 표준 event type

예:

```json
{
  "sourceType": "attemptSaved",
  "sourceSubtype": "OpenPlay",
  "canonicalType": "shot_on_target"
}
```

### 시간 표준화

- `minute`: 경기 분
- `second`: 해당 분 내 초
- `stoppageMinute`: `45+2`의 `2`
- `matchSecond`: 경기 시작 후 누적 초
- `period`: 1, 2, 3(ET), 4(ET), 5(PEN) 등

### 좌표 표준화

- source 좌표는 `sourceLocationX/Y` 등에 보존
- normalized 좌표는 `locationX/Y` 등에 저장
- normalized 좌표는 v2에서 `0~100` 기준으로 통일
- 공격 방향 반전 여부는 source adapter가 결정하고, 규칙은 source별 문서에 명시

## Canonical Event Type 최소 세트

초기 v2에서는 과도한 세분화보다 아래를 권장한다.

- `goal`
- `shot`
- `shot_on_target`
- `assist`
- `pass`
- `key_pass`
- `tackle`
- `interception`
- `clearance`
- `foul`
- `yellow_card`
- `red_card`
- `substitution`
- `own_goal`
- `penalty_won`
- `penalty_scored`
- `penalty_missed`
- `var_decision`

## Source별 기대 수준

### StatsBomb

- `sourceType`, `canonicalType`, `period`, `matchSecond`, 좌표 모두 채울 수 있음
- `metrics.xg`도 안정적으로 채울 수 있음

### SofaScore

- incidents 기반이므로 좌표는 비어 있을 수 있음
- `canonicalType`, `minute`, `detail` 중심

### Understat

- shot 중심 source
- `canonicalType`은 `shot`, `goal`
- `metrics.xg` 필수
- `sourceType`/`sourceSubtype`는 `result`, `shotType`, `situation` 기반으로 보강

### WhoScored

- `sourceType`/`sourceSubtype` 보존 가치가 큼
- 좌표/shot 정보 풍부
- qualifier를 `sourcePayload`에 그대로 보존 가능

### API-Football

- incidents 중심 source
- 좌표 부족
- `canonicalType`, `minute`, `detail` 중심

## Source-aware Raw 보존 원칙

v2는 normalized contract이지만, source별 원형은 버리지 않는다.

- raw 원형은 `raw_payloads` 또는 `raw_event_bundle` artifact에 보존
- v2 payload에서는 필요한 최소 source 필드만 유지
- 정말 source-specific debugging이 필요하면 `sourcePayload`에 좁은 subset만 둔다

## Migration Plan

### Phase 1

- 문서만 확정
- 기존 `analysis_detail` v1 유지

### Phase 2

- `src/data/types.ts`에 v2 타입 추가
- reader는 v1/v2 둘 다 읽을 수 있게 확장

### 파일명 전략

- transition 동안 파일명은 계속 `analysis-detail.v1.json.gz`를 사용해도 된다.
- 실제 schema version은 payload 내부 `version`으로 구분한다.
- reader는 `version: 1 | 2`를 모두 허용한다.
- 충분히 안정화되면 나중에 `analysis-detail.v2.json.gz`로 올리는 별도 migration을 고려한다.

### Phase 3

- source adapter별 writer를 v2로 업그레이드
  - StatsBomb
  - Understat
  - SofaScore
  - WhoScored
  - API-Football

### Phase 4

- v2 coverage가 충분해지면 reader 기본값을 v2로 전환
- 필요 시 v1은 deprecated

## 현재 코드 기준 영향 지점

- 타입: `src/data/types.ts`
- writer: `src/data/matchEventArtifactWriter.ts`
- source adapter:
  - `src/data/statsbombMaterializeDetails.ts`
  - `src/data/sofascoreDetailsMaterialize.ts`
  - `src/data/apiFootballMatchEventsSync.ts`
  - `src/data/understatShotArtifacts.ts`
  - `src/data/whoscoredMatchArtifacts.ts`
- reader:
  - `src/data/matchEventArtifacts.ts`
  - `src/data/postgres.ts`

## 비목표

- 모든 source가 v2 모든 필드를 즉시 채우는 것
- event table 재도입
- source-specific 모든 qualifier를 canonical schema에 승격하는 것

## 결론

`analysis_detail.v2`는 새 테이블이 아니라 **artifact-first normalized event contract**다.

이 문서의 핵심은:

- source 원형은 보존
- canonical 의미는 별도 필드로 명시
- 시간/좌표는 초기에 표준화
- derived metric은 최소한만 포함

즉 MatchIndex는 raw source와 normalized event를 같은 payload 안에서 구분하되, layer 관점에서는 분명히 분리된 구조로 간다.
