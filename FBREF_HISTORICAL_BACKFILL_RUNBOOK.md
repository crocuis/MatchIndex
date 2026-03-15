# FBREF_HISTORICAL_BACKFILL_RUNBOOK.md

## 목적

FBref historical/backfill 데이터를 `soccerdata` collector 기준으로 수집하고 `player_season_stats`까지 적재하는 실행 순서를 정리한다.

## 권장 경로

```text
soccerdata collector -> JSONL 파일 -> soccerdata raw import -> soccerdata FBref materialize
```

## 1. collector dry-run

```bash
npm run soccerdata:collect-fbref -- --competition=PL --season=2024-2025
```

확인 포인트:

- `plannedDatasets`
- `cookieConfigured`
- competition / season 값

## 2. collector write

```bash
npm run soccerdata:collect-fbref -- --competition=PL --season=2024-2025 --write --output=data/fbref-pl-2024-2025.jsonl
```

### Cloudflare/403 대응

- `SOCCERDATA_COOKIE` 또는 `--cookie-file` 사용 권장
- 필요 시 `SOCCERDATA_PROXY` 사용
- direct fetch 경로 대신 `soccerdata` 또는 Playwright 보조 collector를 우선 사용

### 현재 검증 결과

- direct `fbrefPlayerStatsIngest` 경로: `403`
- `soccerdata-collect-fbref.py` 경로: `https://fbref.com/en/comps/`에서 `403`
- `soccerdata-collect-fbref-playwright.mts` 경로: `FBref challenge did not clear`

즉 쿠키 없이 자동 수집은 현재 실패했다.

## 2-1. 쿠키 기반 재시도

가장 현실적인 다음 시도는 브라우저에서 Cloudflare를 통과한 뒤 쿠키를 collector에 주입하는 것이다.

### soccerdata collector

```bash
python3 scripts/soccerdata-collect-fbref.py \
  --competition=PL \
  --season=2024-2025 \
  --cookie-file=/path/to/fbref-cookie.txt \
  --write \
  --output=data/fbref-pl-2024-2025.jsonl
```

또는:

```bash
export SOCCERDATA_COOKIE="cf_clearance=...; other_cookie=..."
```

### 쿠키 파일 형식

collector는 아래처럼 단순 `Cookie` 헤더 문자열을 읽을 수 있으면 된다.

```text
cf_clearance=...; __cf_bm=...; other_cookie=...
```

권장 파일명 예시:

```text
data/fbref-cookie.txt
```

### Playwright collector

```bash
node --experimental-strip-types scripts/soccerdata-collect-fbref-playwright.mts \
  --competition=PL \
  --season=2024-2025 \
  --cookie-file=/path/to/fbref-cookie.txt \
  --write \
  --output=data/fbref-pl-2024-2025-playwright.jsonl
```

필요 시 디버깅용:

```bash
node --experimental-strip-types scripts/soccerdata-collect-fbref-playwright.mts \
  --competition=PL \
  --season=2024-2025 \
  --headed \
  --cookie-file=/path/to/fbref-cookie.txt \
  --write \
  --output=data/fbref-pl-2024-2025-playwright.jsonl
```

## 2-2. 수동 CSV fallback

쿠키 기반 수집도 실패하면 historical backfill은 수동 CSV export로 진행한다.

권장 순서:

1. 브라우저에서 FBref 시즌 테이블 열기
2. `Share & Export -> Get table as CSV`
3. category별 CSV 저장
4. 별도 importer로 `raw_payloads` 또는 직접 materialize 입력으로 변환

## 현재 권장 우선순위

1. `soccerdata + cookie-file`
2. `Playwright + cookie-file`
3. 수동 CSV export

## WSL / Windows 비교 진단 절차

현재 실패 원인은 주로 Cloudflare 차단이지만, WSL 환경이 우회 난이도를 높일 수 있다. 아래 순서로 비교 진단한다.

### 1. Windows 본체 브라우저 확인

- Windows Chrome/Edge에서 `https://fbref.com/en/comps/9/Premier-League-Stats` 접속
- challenge 없이 페이지가 열리는지 확인
- 열리면 브라우저 세션은 유효한 상태다

### 2. WSL direct/soccerdata 비교

WSL에서:

```bash
npm run player-stats:ingest-fbref -- --competition=PL --season=2024-2025 --write
python3 scripts/soccerdata-collect-fbref.py --competition=PL --season=2024-2025 --write --output=data/fbref-pl-2024-2025.jsonl
```

확인 포인트:

- 둘 다 `403`이면 환경보다는 Cloudflare 차단 가능성이 큼
- Playwright만 실패하면 브라우저 fingerprint/헤드리스 challenge 가능성 큼

### 3. Windows 쿠키 -> WSL collector 재시도

- Windows 브라우저에서 FBref challenge를 통과한 뒤 쿠키를 export
- WSL에서 `--cookie-file` 또는 `SOCCERDATA_COOKIE`로 재실행
- 성공하면 "WSL 자체 문제"가 아니라 "세션 없는 요청 차단"으로 판단

### 4. Windows 네이티브 실행 비교

가능하면 동일 collector를 Windows PowerShell/Command Prompt에서도 한 번 실행한다.

비교 기준:

- Windows native 성공 + WSL 실패: WSL/browser fingerprint 영향 있음
- 둘 다 실패: source 차단/세션 문제

## Windows 브라우저 쿠키 -> WSL 재실행 절차

### 1. Windows 브라우저에서 FBref 접속

- Chrome 또는 Edge에서 FBref 접속
- `Just a moment...`가 사라지고 실제 페이지가 열릴 때까지 대기

### 2. 쿠키 추출

방법은 아무거나 괜찮지만, 결과는 최종적으로 아래 형식이면 된다.

```text
cf_clearance=...; __cf_bm=...; other_cookie=...
```

가장 쉬운 방식:

- 브라우저 DevTools -> Application/Storage -> Cookies -> `https://fbref.com`
- 필요한 쿠키 값을 복사해 한 줄로 정리
- WSL 프로젝트 안 `data/fbref-cookie.txt`에 저장

### 3. WSL에서 soccerdata collector 재실행

```bash
python3 scripts/soccerdata-collect-fbref.py \
  --competition=PL \
  --season=2024-2025 \
  --cookie-file=data/fbref-cookie.txt \
  --write \
  --output=data/fbref-pl-2024-2025.jsonl
```

### 4. WSL에서 Playwright collector 재실행

```bash
node --experimental-strip-types scripts/soccerdata-collect-fbref-playwright.mts \
  --competition=PL \
  --season=2024-2025 \
  --cookie-file=data/fbref-cookie.txt \
  --write \
  --output=data/fbref-pl-2024-2025-playwright.jsonl
```

디버그가 필요하면:

```bash
node --experimental-strip-types scripts/soccerdata-collect-fbref-playwright.mts \
  --competition=PL \
  --season=2024-2025 \
  --cookie-file=data/fbref-cookie.txt \
  --headed \
  --write \
  --output=data/fbref-pl-2024-2025-playwright.jsonl
```

### 5. 성공 후 다음 단계

파일이 생기면 바로 이어서:

```bash
npm run soccerdata:import-raw -- --competition=PL --season=2024-2025 --input=data/fbref-pl-2024-2025.jsonl --write
npm run soccerdata:materialize-fbref -- --competition=PL --season=2024-2025 --write
```

## 3. raw import

```bash
npm run soccerdata:import-raw -- --competition=PL --season=2024-2025 --input=data/fbref-pl-2024-2025.jsonl --write
```

이 단계는:

- `raw_payloads`
- `source_sync_manifests`

를 채운다.

## 4. materialize dry-run

```bash
npm run soccerdata:materialize-fbref -- --competition=PL --season=2024-2025
```

확인 포인트:

- `rawPayloadsRead`
- `rowsPlanned`
- `unmatchedExternalPlayerIds`

## 5. materialize write

```bash
npm run soccerdata:materialize-fbref -- --competition=PL --season=2024-2025 --write
```

이 단계는:

- `player_season_stats`
- `mv_top_scorers`

를 갱신한다.

## 현재 한계

- `soccerdataFbrefMaterialize.ts`는 현재 `player_season_stats_standard` dataset만 사용한다.
- FBref direct fetch는 403이 발생할 수 있으므로 운영 주 경로로 권장하지 않는다.
- canonical player 매핑 품질은 `source_entity_mapping` 또는 roster heuristic 품질에 따라 달라진다.

## 후속 작업

1. FBref source mapping 정교화
2. 추가 stat category (`shooting`, `passing`, `defense`) materialize 확장
3. owner matrix 적용
