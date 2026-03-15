# MATCH_EVENT_TABLE_REMOVAL_CHECKLIST.md

## 목적

`match_events`, `match_event_relations`, `match_event_freeze_frames`, `match_event_visible_areas` 제거 전에 확인해야 할 최종 항목을 정리한다.

## 현재 상태

- `analysis_detail`, `freeze_frames`, `visible_areas`는 GitHub artifact 기반으로 읽는다.
- `getMatchTimelineDb()`는 artifact 우선, DB fallback 구조다.
- `MATCHINDEX_ENABLE_PLAYER_SEASON_EVENT_FALLBACK`를 설정하지 않으면 `match_events` 기반 선수 시즌 fallback은 비활성화된다.

## 2026-03-15 점검 결과

legacy fallback 비활성 기준 주요 roster path에서 `player_season_stats` 공백이 아직 남아 있다.

```json
{
  "players_latest_contract_missing_stats": 3785,
  "club_players_missing_stats": 3837,
  "nation_players_missing_stats": 3785,
  "total_player_contract_roster_rows": 17715
}
```

추가로 최신 선수 경로에서 `appearances/goals/assists/minutes_played`가 모두 0인 선수도 `3817`명이다.

즉, 지금 `match_events` 기반 fallback을 영구 제거하면 선수/클럽/국가 페이지에서 시즌 스탯 공백이 대량으로 생길 수 있다.

## 삭제 전 필수 조건

### 1. Timeline artifact 전환 완료

- [x] `getMatchTimelineDb()`가 artifact 전용으로 동작
- [ ] DB fallback 제거 전에 notable event 타입이 artifact에 충분히 들어있는지 전수 검증
- [ ] `goal`, `own_goal`, `penalty_scored`, `penalty_missed`, `yellow_card`, `red_card`, `yellow_red_card`, `substitution`, `var_decision`가 모두 artifact에 보존되는지 샘플 점검

### 2. Player season stats coverage 확보

- [ ] `player_season_stats` 커버리지 100% 또는 허용 가능한 수준까지 확보
- [ ] `players_latest_contract_missing_stats = 0` 확인
- [ ] `club_players_missing_stats = 0` 확인
- [ ] `nation_players_missing_stats = 0` 확인
- [ ] `empty_stats_players = 0` 또는 의도된 예외 목록 확정

### 3. Materialize/sync event write 제거 완료

- [x] `apiFootballMatchEventsSync.ts`가 artifact 생성 중심으로 전환됨
- [x] `sofascoreDetailsMaterialize.ts`가 event DB write 대신 artifact 생성 사용
- [x] `statsbombMaterializeDetails.ts`가 event/freeze/visible DB write 대신 artifact 생성 사용
- [x] 관련 스크립트를 실제 운영 배치에서 한 번씩 실행해 artifact 생성이 정상 동작하는지 확인

### 4. Relational read model 유지 항목 확인

아래는 제거 대상이 아니다.

- [ ] `match_lineups`
- [ ] `match_stats`
- [ ] `player_season_stats`
- [ ] `player_contracts`
- [ ] `team_seasons`

### 5. Rollback 경로 확보

- [x] artifact 재생성은 legacy export가 아니라 source pipeline 재실행으로 수행하도록 전환
- [ ] `football_data` 저장소에 전체 artifact가 최신 상태로 push 되었는지 확인
- [ ] 문제 발생 시 `MATCHINDEX_ENABLE_PLAYER_SEASON_EVENT_FALLBACK=true`로 임시 복구 가능 여부 확인

## 삭제 순서 권장안

1. `player_season_stats` coverage 확보
2. artifact 기반 timeline 전수 검증
3. 운영 배치에서 materialize/sync 재실행
4. `match_event_relations` 제거
5. `match_event_freeze_frames` 제거
6. `match_event_visible_areas` 제거
7. `match_events` 제거
8. DB fallback 코드 제거

## 삭제 직후 확인 항목

- [ ] `/matches/[id]?tab=events` 정상 렌더
- [ ] `/matches/[id]?tab=analysis` 정상 렌더
- [ ] 선수 상세 페이지 시즌 스탯 정상 노출
- [ ] 클럽 선수단 페이지 시즌 스탯 정상 노출
- [ ] 국가 선수 목록 페이지 시즌 스탯 정상 노출
- [ ] 전체 타입체크 통과
- [ ] artifact export 스크립트 dry-run/write 정상 동작
