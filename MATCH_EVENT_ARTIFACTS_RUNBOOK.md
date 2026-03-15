# MATCH_EVENT_ARTIFACTS_RUNBOOK.md

## 목적

소스 파이프라인이 생성한 경기 단위 `JSON.gz` artifact를 Git으로 관리하고, 앱은 필요 시 artifact를 우선 읽도록 운영한다.

현재 1차 대상 artifact는 아래 3종이다.

- `analysis_detail`
- `freeze_frames`
- `visible_areas`

## 파일 저장 규칙

- 기본 루트 디렉터리: `artifacts/`
- 환경 변수 override: `MATCH_EVENT_ARTIFACTS_DIR`
- 앱 기본 read source: `jsDelivr` (`https://cdn.jsdelivr.net/gh/crocuis/football_data@main`)
- 원격 base URL override: `MATCH_EVENT_ARTIFACT_REMOTE_BASE_URL`
- 읽기 모드 override: `MATCH_EVENT_ARTIFACT_READ_MODE` (`remote-first`, `local-first`, `remote-only`)
- private GitHub fallback: `MATCH_EVENT_ARTIFACT_GITHUB_TOKEN` + `MATCH_EVENT_ARTIFACT_GITHUB_OWNER/REPO/REF`
- 저장 경로 형식:

```text
artifacts/<source>/matches/<year>/<month>/<matchId>/analysis-detail.v1.json.gz
artifacts/<source>/matches/<year>/<month>/<matchId>/freeze-frames.v1.json.gz
artifacts/<source>/matches/<year>/<month>/<matchId>/visible-areas.v1.json.gz
```

## 사전 조건

### 1. DB migration 적용

먼저 `match_event_artifacts` 테이블을 생성해야 한다.

```bash
psql "$DATABASE_URL" -f db/migrations/012_match_event_artifacts.sql
```

확인 쿼리:

```bash
psql "$DATABASE_URL" -c "\d match_event_artifacts"
```

### 2. artifact 디렉터리 준비

기본 경로를 쓸 경우 별도 준비는 필요 없다. `--write` 실행 시 디렉터리를 자동 생성한다.

다른 경로를 쓰려면:

```bash
export MATCH_EVENT_ARTIFACTS_DIR=./artifacts
export MATCH_EVENT_ARTIFACT_REMOTE_BASE_URL=https://cdn.jsdelivr.net/gh/crocuis/football_data@main
export MATCH_EVENT_ARTIFACT_READ_MODE=remote-first
# private repo를 유지하면 아래도 함께 설정
# export MATCH_EVENT_ARTIFACT_GITHUB_OWNER=crocuis
# export MATCH_EVENT_ARTIFACT_GITHUB_REPO=football_data
# export MATCH_EVENT_ARTIFACT_GITHUB_REF=main
# export MATCH_EVENT_ARTIFACT_GITHUB_TOKEN=ghp_xxx
```

## 권장 실행 순서

### 1. source pipeline에서 artifact 생성

```bash
npm run statsbomb:materialize-details
npm run soccerdata:materialize-sofascore-details
npm run match-events:sync
```

확인 포인트:

- 생성된 artifact 파일 수
- `match_event_artifacts` metadata upsert 여부
- source pipeline 로그의 대상 경기 수

artifact는 source pipeline 재실행으로 갱신한다.

예시 source:

- `statsbomb`
- `sofascore`
- `understat`
- `whoscored`
- `api_football`

### 3. 생성 결과 확인

파일 확인:

```bash
ls artifacts/statsbomb/matches/2024/07/3943077
```

메타 확인:

```bash
psql "$DATABASE_URL" -c "
  SELECT match_id, artifact_type, format, storage_key, version, row_count, byte_size
  FROM match_event_artifacts
  WHERE match_id = 3943077
  ORDER BY artifact_type, version DESC;
"
```

## Git 반영 절차

artifact는 Git으로 관리하므로, 생성 후 아래 순서로 검토한다.

### 1. 변경 파일 확인

```bash
git status --short artifacts/
```

### 2. 샘플 압축 해제 확인

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path

path = Path('artifacts/statsbomb/matches/2024/07/3943077/analysis-detail.v1.json.gz')
with gzip.open(path, 'rt', encoding='utf-8') as fh:
    payload = json.load(fh)

print(payload['artifactType'], payload['matchId'], len(payload['events']))
PY
```

### 3. staged diff 검토

```bash
git add artifacts/statsbomb/matches/2024/07/3943077
git diff --cached --stat
```

### 4. 커밋 전략

- 경기 수가 적으면 source code 변경과 같은 커밋에 포함 가능
- 경기 수가 많으면 artifact-only 커밋으로 분리 권장
- 대량 적재가 반복되면 Git LFS 또는 별도 artifact repo 검토

## 운영 원칙

- artifact는 `match_id` 단위로만 생성한다.
- 앱은 artifact가 있으면 `jsDelivr` 원격 URL을 우선 읽고, private 저장소이거나 CDN 접근이 안 되면 GitHub Contents API + token 경로를 사용한다. 둘 다 실패하면 로컬 파일 fallback을 사용한다.
- `freeze_frames`, `visible_areas`도 현재 UI read path에 연결되어 있다.
- 재생성 가능 데이터이므로 파일 손상 시 source pipeline을 다시 실행한다.

## 장애 대응

### `match_event_artifacts` 테이블이 없다고 나올 때

마이그레이션이 적용되지 않은 상태다.

```bash
psql "$DATABASE_URL" -f db/migrations/012_match_event_artifacts.sql
```

### artifact 파일은 생겼는데 DB 메타가 비어 있을 때

- source pipeline이 `match_event_artifacts` metadata upsert를 수행하는지 확인
- DB 권한과 `DATABASE_URL` 확인
- 해당 source pipeline을 다시 실행

### 원격 artifact가 404로 읽히지 않을 때

- public 저장소라면 `MATCH_EVENT_ARTIFACT_REMOTE_BASE_URL`이 실제 브랜치/ref와 맞는지 확인
- private 저장소라면 `MATCH_EVENT_ARTIFACT_GITHUB_TOKEN`을 설정해야 한다
- token은 대상 저장소 read 권한이 있어야 한다

### rowCount가 0으로 나올 때

- 해당 경기에 좌표 기반 analysis event 또는 freeze frame / visible area 데이터가 실제 없는 경우일 수 있다.
- StatsBomb/Sofascore detail materialize가 먼저 되었는지 확인한다.

## 현재 한계

- `analysis_detail`, `freeze_frames`, `visible_areas` 모두 앱 read path에 연결돼 있다.
- locale name resolution은 artifact에 저장하지 않고 기존 Postgres 참조를 사용한다.
- Git에 binary-like 압축 파일이 누적되므로 장기적으로 저장소 비대화 관리가 필요하다.
