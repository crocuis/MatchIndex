# FBREF_INGEST_MATERIALIZE_CONTRACT.md

## 목적

MatchIndex에서 FBref 기반 시즌 스탯 소스를 도입할 때 필요한 ingest/materialize 계약을 고정한다.

이 문서는 `football-data-webscraping`의 FBref scraping 패턴을 참고하되, MatchIndex 내부 canonical 파이프라인에 맞는 구현 계약만 정의한다.

## 전제

- FBref는 `player_season_stats`의 current season primary source가 아니다.
- FBref는 historical/backfill source 또는 owner가 비어 있는 `competition_season` 보강용이다.
- canonical write 대상은 여전히 `player_season_stats` 하나다.

## 파이프라인 경계

```text
FBref source -> raw_payloads -> source_entity_mapping -> player_season_stats -> mv_top_scorers
```

## 1. Source Registration Contract

### `data_sources`

FBref adapter는 아래 source를 보장해야 한다.

- `slug`: `fbref_scrape`
- `name`: `FBref Scrape`
- `base_url`: `https://fbref.com`
- `source_kind`: `scrape`
- `priority`: historical/backfill 용도에 맞는 낮은 우선순위

## 2. Ingest Contract

### 역할

- 시즌/리그/stat category 단위로 FBref 페이지를 수집한다.
- 원본 응답을 `raw_payloads`에 저장한다.
- 필요 시 `source_sync_manifests`에 시즌별 진행 상태를 남긴다.

### 입력 파라미터

- `competitionCode`
- `season`
- `statCategory`
- `dryRun`

### 수집 단위

최소 수집 카테고리:

- `standard`
- `shooting`
- `passing`
- `defense`
- `possession`
- 필요 시 `misc`, `keeper`

### `raw_payloads` 저장 계약

#### 권장 `entity_type`

- `player`

#### 권장 `endpoint`

```text
fbref:/comps/<competition>/<season>/<category>/players
```

#### 권장 `season_context`

- `2024-2025` 또는 `2024` 중 하나로 통일
- MatchIndex 내부 `competition_seasons` 매핑 로직과 동일해야 함

#### `payload` 최소 구조

```json
{
  "source": "fbref",
  "competitionCode": "PL",
  "season": "2024-2025",
  "statCategory": "standard",
  "pageUrl": "https://fbref.com/...",
  "fetchedAt": "2026-03-15T00:00:00.000Z",
  "columns": ["Player", "Squad", "Nation", "Age", "Min", "Gls", "Ast"],
  "rows": [
    {
      "playerName": "...",
      "teamName": "...",
      "fbrefPlayerId": "optional-if-extracted",
      "fbrefPlayerUrl": "optional",
      "values": {
        "minutes": 1234,
        "goals": 5,
        "assists": 2
      }
    }
  ]
}
```

### ingest 단계 요구사항

- dry-run 지원
- 재실행해도 `raw_payloads` 중복 적재를 제어할 수 있어야 함 (`payload_hash` 사용 권장)
- category별 payload를 분리 저장
- 원본 column name을 보존해 디버깅 가능해야 함

## 3. Mapping Contract

### 목적

FBref player/team/season 식별자를 MatchIndex canonical entity에 연결한다.

### 필요한 매핑

- FBref player -> `players.id`
- FBref team -> `teams.id`
- FBref competition+season -> `competition_seasons.id`

### 매핑 우선순위

1. 기존 `source_entity_mapping` direct hit
2. player name + current/historical team + season 교차 검증
3. DOB/국적/포지션 보조 매칭
4. 실패 건은 quarantine

### `source_entity_mapping` 계약

#### player mapping

- `entity_type`: `player`
- `external_id`: `fbrefPlayerId`가 있으면 우선 사용
- 없으면 `fbrefPlayerUrl` 또는 정규화된 compound key를 metadata에 보관

#### team mapping

- `entity_type`: `team`
- external team key를 별도 보관

### 매핑 실패 정책

- 자동 매핑 confidence가 낮으면 `player_season_stats` upsert 금지
- 실패 건 summary JSON 출력
- source row는 남기되 canonical write는 건너뜀

## 4. Materialize Contract

### 입력

- `raw_payloads`에서 읽은 FBref category payload
- `source_entity_mapping`
- `competition_seasons`

### 출력

- `player_season_stats`
- `mv_top_scorers` refresh

### category merge 규칙

FBref는 category별 페이지가 분리돼 있으므로, materialize 전에 아래 key로 merge해야 한다.

```text
(competitionSeasonId, canonicalPlayerId)
```

필요 시 보조 key:

```text
(fbrefPlayerId, teamId, season)
```

### 필드 매핑 목표

#### 필수 1차 필드

- `appearances`
- `starts`
- `minutes_played`
- `goals`
- `assists`
- `yellow_cards`
- `red_cards`

#### 가능하면 채울 필드

- `penalty_goals`
- `own_goals`
- `yellow_red_cards`
- `clean_sheets`
- `goals_conceded`
- `saves`

#### 기본 전략

- FBref에서 안정적으로 직접 얻을 수 있는 값만 채운다
- 불명확한 값은 0 또는 `NULL`로 두고, 다른 source owner 시즌을 덮어쓰지 않는다

### upsert 정책

- `UNIQUE (player_id, competition_season_id)` 기준 idempotent upsert
- 단, owner 없는 시즌 또는 FBref backfill 대상 시즌에만 write
- current/recent API-Football owner 시즌은 patch하지 않음

## 5. Source Owner Rule

### FBref가 write 가능한 경우

- `competition_season` owner가 비어 있음
- 또는 명시적으로 `fbref_backfill` 대상 시즌으로 지정됨

### FBref가 write하면 안 되는 경우

- API-Football current/recent owner 시즌
- StatsBomb open data owner 시즌
- source conflict 해소 정책이 정해지지 않은 시즌

## 6. CLI Contract

### ingest script

예시:

```bash
node --experimental-strip-types scripts/fbref-ingest-player-stats.mts --competition=PL --season=2024-2025 --write
```

요구사항:

- `--competition`
- `--season`
- `--category` 또는 category 전체 순회
- `--write`
- dry-run 기본

### materialize script

예시:

```bash
node --experimental-strip-types scripts/fbref-materialize-player-stats.mts --competition=PL --season=2024-2025 --write
```

요구사항:

- `--competition`
- `--season`
- `--write`
- dry-run 기본
- unmatched player/team summary 출력

## 7. 수용 기준

- FBref payload를 `raw_payloads`에 저장할 수 있다.
- 동일 시즌/리그/category 재실행이 idempotent 하다.
- `source_entity_mapping` 기반으로 canonical player 연결이 가능하다.
- 특정 `competition_season`에 대해 `player_season_stats`를 upsert할 수 있다.
- `mv_top_scorers` refresh 후 리그 top scorers가 갱신된다.

## 8. 비목표

- `football-data-webscraping` 전체 저장소 vendor import
- SofaScore/WhoScored/Understat/Transfermarkt를 같은 adapter에서 한 번에 통합
- current season primary owner 대체
- raw HTML/DataFrame 구조를 앱이 직접 읽는 구조

## 다음 구현 순서

1. `fbref_scrape` source 등록 helper 추가
2. FBref raw ingest script 구현
3. FBref player/team mapping helper 구현
4. FBref materialize 구현
5. owner matrix 적용
