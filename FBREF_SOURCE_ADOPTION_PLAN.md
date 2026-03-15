# FBREF_SOURCE_ADOPTION_PLAN.md

## 목적

`sahil-gidwani/football-data-webscraping`를 MatchIndex에 어떻게 활용할지 범위를 고정한다.

결론부터 말하면, 이 저장소를 통째로 도입하지 않고 **FBref scraping reference만 선택적으로 차용**한다.

## 도입 범위 결론

### 채택

- `fbref/` 폴더의 scraping 접근 방식
- FBref 시즌별 선수/팀 집계 페이지 URL 패턴
- HTML table 기반 수집 패턴 (`requests` + `pandas.read_html()` 또는 대체 파서)

### 채택하지 않음

- `sofascore/`
- `whoscored/`
- `transfermarkt/`
- `understat/`
- 저장소 전체를 MatchIndex의 단일 통합 소스로 간주하는 방식

## 왜 전체 도입이 아닌가

### 1. 저장소 성격

- 이 저장소는 프로덕션 ingest/materialize 시스템이 아니라 교육용 scraping toolkit이다.
- 배치 실행, sync run 추적, raw payload 적재, canonical upsert, 재실행 안정성 같은 MatchIndex 핵심 요구사항이 없다.

### 2. MatchIndex와의 경계 불일치

MatchIndex는 아래 경계를 이미 갖고 있다.

```text
external source -> raw_payloads -> source_entity_mapping -> player_season_stats -> mv_top_scorers
```

`football-data-webscraping`는 이 계약을 만족하지 않는다.

- `raw_payloads` 저장 로직 없음
- `source_entity_mapping` 연결 없음
- `player_season_stats` upsert 없음
- season owner / source precedence 없음

### 3. source별 성격이 다름

- FBref: 시즌 집계 테이블
- Understat: shot-level xG 이벤트
- SofaScore: match-level data
- WhoScored: event data
- Transfermarkt: profile/market value

즉 "여러 소스를 지원한다"는 장점은 있지만, MatchIndex가 원하는 `player_season_stats` canonical source로는 인터페이스가 통일되어 있지 않다.

## MatchIndex 기준 권장 역할

### FBref

- 역할: `player_season_stats` historical/backfill source
- 범위: API-Football owner가 없는 시즌, historical season, big-five 중심 시즌 보강
- 우선순위: owner 없는 `competition_season`만 채우거나 명시적 backfill cohort만 채운다

### API-Football

- 역할: current/recent season primary owner
- 이유: 시즌 집계 API 구조가 이미 materialize 패턴과 잘 맞는다

### StatsBomb

- 역할: open-data season owner
- 이유: 이벤트 기반 집계가 정확하고 이미 구현됨

### SofaScore / WhoScored / Understat / Transfermarkt

- 역할: 별도 read model 또는 메타데이터/고급지표 source
- `player_season_stats` primary source로 사용하지 않음

## 도입 원칙

### 원칙 1. 저장소 도입이 아니라 adapter 구현

- MatchIndex 안에 FBref source adapter를 구현한다.
- 외부 저장소는 참고 구현(reference)일 뿐, runtime dependency로 두지 않는다.

### 원칙 2. canonical contract 유지

아래 경계는 유지한다.

- `raw_payloads`
- `source_sync_runs`
- `source_sync_manifests`
- `source_entity_mapping`
- `player_season_stats`

### 원칙 3. season owner matrix 유지

- 한 `competition_season`에는 source owner를 하나만 둔다.
- FBref는 owner가 비어 있는 season만 채우거나, historical backfill 대상으로만 실행한다.
- FBref가 API-Football current season row를 patch하는 구조는 피한다.

## 실제 채택 대상 기능

### 차용할 것

- FBref URL 패턴
- FBref stat category 접근 방식
- HTML table scraping 패턴
- 필요 시 JS-rendered fallback 아이디어

### MatchIndex에서 직접 구현할 것

- source 등록 (`data_sources`)
- raw payload 저장 (`raw_payloads`)
- player/team/competition season mapping (`source_entity_mapping`)
- idempotent materialize (`player_season_stats`)
- `mv_top_scorers` refresh
- dry-run / write CLI

## 성공 기준

- FBref source adapter가 MatchIndex 내부 파이프라인에 맞게 독립적으로 동작한다.
- 외부 저장소 구조 변경 없이 MatchIndex가 계속 운영 가능하다.
- FBref는 `player_season_stats` historical backfill source로 동작한다.
- `football-data-webscraping` 전체 저장소를 vendor처럼 끌어오지 않는다.

## 다음 단계

1. MatchIndex용 FBref ingest/materialize contract 확정
2. `soccerdataFbrefMaterialize.ts` 스캐폴드 대체 또는 확장 설계
3. source owner matrix 문서화
