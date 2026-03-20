# PLAYER_CONTRACTS_BACKFILL_RUNBOOK.md

## 목적

이미 적재된 시즌 데이터에 대해 `player_contracts`와 `team_seasons`를 보강해, 구단 페이지의 시즌별 선수단이 비는 구간을 줄인다.

## 적용 대상

- API-Football 선수 스탯 적재분
- Sofascore 상세(lineups) 적재분

## 권장 실행 순서

### 1. API-Football 선수 스탯 materialize 실행

이 단계는 이제 `player_season_stats` upsert와 함께 `player_contracts` backfill도 같이 수행한다.

```bash
npm run player-stats:materialize-api-football -- 2024 2025 --competitions=PL,PD,SA,BL1,FL1,UCL,UEL --write
```

- 특정 리그만 처리하려면 `--competitions=PL`처럼 줄인다.
- 계약 백필을 일부러 제외해야 할 때만 `--skip-contract-backfill`을 붙인다.

### 2. Sofascore 상세 materialize 실행

이 단계는 match lineups/event/stat 적재와 함께 lineup 기반 `player_contracts`를 upsert한다.

```bash
npm run soccerdata:materialize-sofascore-details -- --competition=UEL --season=2025-2026 --write
```

- 현재 스크립트는 `UEL` 기준으로 연결돼 있다.
- 시즌 라벨은 `2025-2026` 형식을 사용한다.

### 3. 필요 시 StatsBomb 상세 materialize 실행

StatsBomb는 이미 `player_contracts` 적재가 구현되어 있으므로, 해당 소스를 다시 적재해야 하는 시즌만 실행한다.

```bash
npm run statsbomb:materialize-details -- --write
```

## 운영 원칙

- 같은 시즌을 다시 돌려도 `player_contracts`는 `ON CONFLICT`로 갱신된다.
- API-Football을 먼저 돌리고, 그다음 Sofascore/StatsBomb를 보강 소스로 쓰는 순서를 권장한다.
- 이유는 API-Football이 시즌 단위 스탯 집계를 먼저 채우고, Sofascore/StatsBomb는 lineup 기반 membership를 추가 보강하는 성격이 강하기 때문이다.

## 확인 포인트

실행 후에는 아래를 우선 확인한다.

- 구단 페이지 시즌 선택 시 선수단이 비어 있지 않은지
- materialize summary에 `contractRows` 또는 `contractBackfill` 수치가 기대치대로 찍히는지
- 특정 시즌에서 여전히 비어 있다면, 그 시즌은 source raw payload 자체에 lineup/player 데이터가 없는지 확인할 것

## 현재 한계

- `football-data.org` 파이프라인은 현재 squad/player raw 데이터를 적재하지 않아서, 같은 방식으로 `player_contracts`를 바로 채울 수 없다.
- football-data 쪽은 `/teams/{id}` 기반 squad ingest 또는 별도 player ingest를 먼저 추가해야 한다.

## Transfermarkt 보강 경로

- active squad에서 방출/이탈 선수를 더 정확히 걸러내려면 `player_transfers`와 `player_contracts.left_date`를 Transfermarkt로 보강할 수 있다.
- 현재는 `npm run player-data:auto-populate -- --competition=<slug> --season=<slug> --team=<slug>`가 club 단위로 계약 정보와 Transfermarkt transfer history sync를 함께 수행한다.
- 예시:

```bash
npm run player-data:auto-populate -- --competition=la-liga --season=2025/26 --team=barcelona
```

- 단, transfer history 단계는 선수별 `Transfermarkt sourceUrl` 매핑이 있어야 실제 호출이 가능하다.
- 매핑이 없으면 계약 sync는 진행돼도 transfer history는 건너뛴다.
