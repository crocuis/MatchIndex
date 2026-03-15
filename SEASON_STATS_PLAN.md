# 시즌별 리그/선수 스탯 적재 플랜

## 목표

- 리그 페이지와 선수 페이지에서 사용하는 시즌별 스탯을 외부 소스에서 수집해 PostgreSQL canonical 모델에 적재한다.
- 기존 패턴을 유지한다: `raw_payloads` 저장 -> canonical/materialized 테이블 upsert -> `src/data/postgres.ts` 조회.
- 구현 범위는 `competition_seasons`, `player_season_stats`, 순위/득점 관련 read model을 먼저 완성하는 데 집중한다.

## 현재 상태

- DB 스키마에는 `seasons`, `competition_seasons`, `player_season_stats`, `mv_standings`, `mv_top_scorers`가 이미 있다.
- 앱 읽기 계층은 이미 DB 우선 구조다. 리그 페이지는 `getStandingsByLeagueDb()`, `getTopScorersBySeasonDb()` 등을 사용하고, 선수 페이지는 `getPlayerByIdDb()`에서 `player_season_stats`를 읽는다.
- ingest/materialize 패턴도 이미 있다.
  - 매치/리그 코어: `src/data/footballDataOrgIngest.ts`, `src/data/footballDataOrgMaterialize.ts`
  - StatsBomb 오픈데이터 적재: `src/data/statsbombMaterialize.ts`, `src/data/statsbombMaterializeDetails.ts`
  - 실행 진입점: `scripts/football-data-ingest-manifests.mts`, `scripts/football-data-materialize-core.mts`, `scripts/sync-match-events.mts`

## 권장 구현 순서

### 0. 소스 선택 결론

- 1차 권장 조합은 다음과 같다.
  - 리그/시즌 메타, 참가 팀, 일정/결과: `football-data.org`
  - 선수 시즌 누적 스탯: `API-Football /v3/players`
- 이유
  - 현재 코드베이스에 `football-data.org` ingest/materialize가 이미 있다.
  - `API-Football`은 시즌 + 리그 기준 선수 누적 스탯 조회가 가능하다.
  - `db/schema.sql`과 현재 리그/선수 페이지 요구사항은 xG보다 기본 시즌 집계가 더 우선이다.
- 보조 후보
  - `FBref` 크롤링: 과거 시즌 보강용 배치 후보, 메인 소스로는 비권장
  - `football-data-webscraping`: 전체 저장소 도입이 아니라 `FBref scraping reference`로만 제한적으로 활용

### 0-1. football-data-webscraping 도입 범위

- `football-data-webscraping`는 MatchIndex의 단일 통합 소스로 채택하지 않는다.
- `fbref/` scraping 패턴만 참고하고, MatchIndex 내부의 canonical ingest/materialize 계약에 맞는 adapter를 직접 구현한다.
- 상세 결정은 `FBREF_SOURCE_ADOPTION_PLAN.md`를 따른다.

### 1. 소스 역할 분리

- 리그 시즌 메타/참가 팀/일정/결과는 기존 `football-data.org` 파이프라인을 계속 사용한다.
- 선수 시즌 스탯은 별도 전용 소스에서 가져와 `player_season_stats`를 채운다.
- 핵심 원칙은 소스별 책임 분리다.
  - 리그/시즌 식별: `football_data_org`
  - 선수 시즌 집계: 별도 player-stats source
  - canonical 연결: `source_entity_mapping`

### 2. 적재 파이프라인 추가

- 새 모듈 2개를 추가한다.
  - `src/data/<player-stats-source>Ingest.ts`: 외부 payload 수집, `raw_payloads`/`source_sync_runs`/`source_sync_manifests` 저장
  - `src/data/<player-stats-source>Materialize.ts`: canonical player/competition season 매핑 후 `player_season_stats` upsert
- 실행 스크립트도 기존 패턴대로 분리한다.
  - `scripts/<player-stats-source>-ingest-*.mts`
  - `scripts/<player-stats-source>-materialize-*.mts`

### 3. 매핑 전략

- 선수 스탯 적재의 핵심 리스크는 외부 선수 ID와 내부 canonical player ID 연결이다.
- 반드시 `source_entity_mapping`를 사용한다.
- 매핑 우선순위는 다음 순서로 둔다.
  1. 기존 source mapping direct hit
  2. 이름 + 소속 팀 + 시즌 교차검증
  3. 생년월일 포함 exact/fuzzy 보조 매칭
  4. 실패 건은 수동 검토 큐로 보관
- 자동 매핑 성공률이 낮은 상태에서 `player_season_stats`를 바로 upsert하지 않는다.

### 4. DB 반영 범위

- 1차 적재 대상 컬럼
  - `appearances`, `starts`, `minutes_played`
  - `goals`, `assists`, `penalty_goals`, `own_goals`
  - `yellow_cards`, `red_cards`, `yellow_red_cards`
  - `clean_sheets`, `goals_conceded`, `saves`
  - `avg_rating`
- 리그 페이지 요구사항과 직접 연결되는 파생 결과
  - `mv_top_scorers` refresh
  - 필요 시 도움/평점용 별도 materialized view 추가 검토

### 5. 읽기 계층 연결

- `src/data/postgres.ts`는 이미 `player_season_stats`를 읽고 있으므로, 1차 목표는 조회 로직 수정이 아니라 적재 품질 확보다.
- 단, 다음 보강은 필요할 수 있다.
  - 시즌 기준 선수 상세 조회 함수 분리
  - 특정 시즌의 선수 스탯 조회 함수 추가
  - 리그별 stat leaders 확장 시 assists/rating 기반 쿼리 추가

### 6. 배치/운영

- 기존 scheduled job 패턴을 재사용한다.
- 권장 배치 구성
  - 하루 1회: 시즌 스탯 전체 증분 수집
  - 경기일 새벽/종료 후: 주요 리그 현재 시즌 재동기화
  - 실패 매핑 재처리: 별도 수동/반자동 배치

## 작업 단위

### Phase A. 소스 확정

- 시즌별 player stats 주 소스는 `API-Football /v3/players`로 확정한다.
- 평가 기준
  - league + player season stats 동시 제공 여부
  - rate limit / 비용 / 이용약관
  - external player/team/competition id 일관성
  - 현재 코드베이스와의 연결 비용

### Phase A-1. 매핑 기반 정리

- 기존 `data/api-football-player-mappings.json`을 `source_entity_mapping`의 `player` 레코드로 반영한다.
- 이 단계가 끝나야 이후 season stats materialize에서 외부 player id -> canonical player id 연결을 안정적으로 처리할 수 있다.

### Phase B. Ingest

- 소스 등록 보장 (`data_sources` upsert)
- sync run 생성
- 시즌/리그 단위 payload 수집
- `raw_payloads` 저장
- 필요 시 `source_sync_manifests`에 competition/player season 단위 manifest 저장

### Phase C. Materialize

- 외부 competition/team/player id를 canonical entity에 매핑
- `competition_seasons` 존재 검증
- `player_season_stats` upsert
- 후처리로 materialized view refresh

### Phase D. 운영 스크립트

- dry-run 지원
- competition/seasons 필터 지원
- scheduled job 연결
- 실패 건 summary JSON 출력

## 수용 기준

- 특정 리그/시즌에 대해 외부 payload를 `raw_payloads`에 저장할 수 있다.
- 해당 시즌 선수 스탯을 `player_season_stats`에 idempotent upsert 할 수 있다.
- `mv_top_scorers` refresh 후 리그 페이지 득점 랭킹이 DB 데이터와 일치한다.
- `getPlayerByIdDb()`가 읽는 `seasonStats`가 실제 적재 데이터로 채워진다.
- dry-run, write 모드 모두 CLI에서 검증 가능하다.

## 바로 다음 구현 항목

1. `API-Football` player mapping을 `source_entity_mapping`으로 동기화
2. `API-Football /v3/players` 기반 ingest 모듈 추가
3. `player_season_stats` upsert + `mv_top_scorers` refresh 연결
4. 스크립트와 scheduled job 추가
