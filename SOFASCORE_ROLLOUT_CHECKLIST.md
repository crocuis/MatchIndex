# SOFASCORE_ROLLOUT_CHECKLIST.md

## 목적

Understat + SofaScore broad rollout 전에, `SofaScore-only` 기준으로 안정적으로 확장하는 최소 리스크 운영 순서를 정리한다.

## 현재 권장 범위

### Phase 1

- `PL 2024-2025`
- artifact owner: `sofascore`
- preferred artifact sources:
  - `sofascore`
  - `understat`
  - `whoscored`

### Phase 2

- `BL1`, `PD`, `SA`, `FL1`

### Phase 3

- `UEL`

### Phase 4

- `UCL`

## owner matrix (pilot)

```json
{
  "owners": {
    "playerSeasonStats": "api_football",
    "playerContracts": "sofascore",
    "matchStats": "sofascore",
    "matchArtifacts": "sofascore"
  },
  "preferredArtifactSources": [
    "sofascore",
    "understat",
    "whoscored"
  ],
  "backfillAllowedSources": [
    "fbref"
  ]
}
```

## 실행 순서

### 1. raw collect

```bash
npm run soccerdata:collect-sofascore -- --competition=PL --season=2024-2025 --write --output=data/sofascore-pl-2024-2025.jsonl
```

### 2. raw import

```bash
npm run soccerdata:import-raw -- --competition=PL --season=2024-2025 --input=data/sofascore-pl-2024-2025.jsonl --write
```

### 3. competition materialize

```bash
npm run soccerdata:materialize-sofascore -- --competition=PL --season=2024-2025 --write
```

### 4. details materialize

```bash
npm run soccerdata:materialize-sofascore-details -- --competition=PL --season=2024-2025 --write
```

## 확인 항목

- `match_stats` row 증가
- `player_contracts` row 증가
- `match_event_artifacts` metadata 증가
- `artifacts/sofascore/matches/...` 파일 생성
- 매치 상세 페이지에서 timeline / analysis 정상 렌더

## rollout gate

아래를 만족해야 다음 competition으로 확장한다.

- [ ] `matchArtifacts=sofascore` owner 정책이 실제로 적용됨
- [ ] details materialize가 artifact를 정상 생성
- [ ] `player_contracts` 중복/누락이 대량 발생하지 않음
- [ ] `match_stats` 값이 기존 read model과 크게 어긋나지 않음
- [ ] 대표 경기 3~5개에서 timeline UI 확인 완료
- [ ] `SOFASCORE_DUPLICATE_CLEANUP_PLAN.md` 기준 duplicate cleanup 완료 또는 허용 수준까지 감소

## Understat 처리 원칙

- 현재 단계에서는 owner가 아니다
- shot enrichment source로만 둔다
- broad rollout 이전에는 `preferredArtifactSources` 2순위 이하로 유지한다

## UCL / UEL 주의점

- stage/group naming variation이 domestic league보다 크다
- team / match matching heuristic이 깨질 확률이 더 높다
- domestic league pilot이 안정화되기 전에는 넣지 않는다

## cleanup 우선 원칙

- 다음 competition rollout 전에 `SOFASCORE_DUPLICATE_CLEANUP_PLAN.md`를 먼저 따른다
- 특히 duplicate player / duplicate match 정리가 끝나기 전에는 `BL1`, `PD`, `SA`, `FL1`로 확대하지 않는다
