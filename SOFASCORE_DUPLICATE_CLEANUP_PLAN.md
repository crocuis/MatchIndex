# SOFASCORE_DUPLICATE_CLEANUP_PLAN.md

## 목적

이미 생성된 `sofascore-*` player / duplicate match 데이터를 정리하고, 이후 rollout 전에 canonical data를 안정화하기 위한 cleanup/migration 초안을 정의한다.

## 문제 요약

### 1. duplicate players

- `players.slug LIKE 'sofascore-%'`가 대량 존재
- 원인:
  - 과거 `sofascoreDetailsMaterialize.ts`가 `source_entity_mapping` lookup 없이 새 player를 생성

### 2. duplicate matches

- 동일 `competition_season_id + match_date + home_team_id + away_team_id` 조합의 `matches` row가 중복 존재
- 원인:
  - `sofascoreCompetitionMaterialize.ts`가 team slug mismatch 시 canonical match를 못 찾고 새 match id를 생성

## 원칙

- raw payload와 source artifact는 삭제하지 않는다
- canonical entity만 정리한다
- winner / loser merge 방식으로 정리한다
- cleanup 이후 source pipeline을 다시 돌려 derived/read model을 재생성한다

## Cleanup Phase A. Duplicate Player Cleanup

### 대상 판정

후보:

- `players.slug LIKE 'sofascore-%'`
- 동일/유사한 `known_as`
- 동일 시즌 roster/team context에서 canonical player와 충돌

### winner 선택 규칙

우선순위:

1. `slug`가 source-prefixed가 아닌 player
2. `source_entity_mapping` source 수가 더 많은 player
3. downstream row가 더 많은 player (`player_contracts`, `player_season_stats`, `match_lineups`)
4. 같은 조건이면 더 오래된 id

### loser 처리

loser -> winner로 아래를 이동:

- `source_entity_mapping`
- `entity_aliases`
- `player_contracts`
- `player_season_stats`
- `match_lineups`
- `player_photo_sources`
- `player_market_values`
- `player_transfers`

그리고 loser player row는 삭제 또는 archived slug로 전환

### 구현 형태

- 일괄 destructive cleanup 전에 dry-run report 생성
- `scripts/merge-duplicate-players.mts` 패턴을 재사용하되
  - candidate selection을 `sofascore-*` 중심으로 자동화
  - winner heuristic을 source mapping 기반으로 강화

## Cleanup Phase B. Duplicate Match Cleanup

### 대상 판정

후보:

```sql
competition_season_id + match_date + home_team_id + away_team_id
```

기준 count > 1

### winner 선택 규칙

우선순위:

1. `source_metadata.source != 'sofascore'` match
2. `source_entity_mapping` source 수가 더 많은 match
3. lineups/stats/artifacts가 더 많은 match
4. 같은 조건이면 더 오래된 id

### loser 처리

loser -> winner로 아래를 이동:

- `match_lineups`
- `match_stats`
- `match_event_artifacts`
- `source_entity_mapping`

그리고 loser match row 삭제

### 구현 형태

- `scripts/dedupe-competition-matches.mts`를 확장하거나
- `scripts/cleanup-sofascore-duplicate-matches.mts` 별도 추가

## Cleanup Phase C. Mapping Stabilization

cleanup만 하고 source mapping을 그대로 두면 재발한다.

반드시 같이 해야 할 것:

- `sofascoreCompetitionMaterialize.ts`
  - team/match는 `source_entity_mapping` 우선
- `sofascoreDetailsMaterialize.ts`
  - player는 `source_entity_mapping` 우선
- 필요 시 `entity_aliases` 보강

## 실행 순서

1. duplicate player candidate report 생성
2. duplicate match candidate report 생성
3. player cleanup dry-run
4. match cleanup dry-run
5. player cleanup write
6. match cleanup write
7. `soccerdata-materialize-sofascore` 재실행
8. `soccerdata-materialize-sofascore-details` 재실행
9. artifact / read model 재검증

## 검증 쿼리

### duplicate players

```sql
SELECT COUNT(*)::int
FROM players
WHERE slug LIKE 'sofascore-%';
```

### duplicate matches

```sql
SELECT competition_season_id, match_date, home_team_id, away_team_id, COUNT(*)::int AS cnt
FROM matches
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;
```

### rollout gate after cleanup

- duplicate `sofascore-*` players 증가가 rerun 후 0
- duplicate matches 신규 증가가 rerun 후 0
- `player_contracts` / `match_stats` / `match_event_artifacts` row count 유지
- `PL` sample match 3~5개 UI 정상

## 남은 rollout 조건

다음 리그 확장 전에 만족해야 한다.

- [x] rerun 시 신규 duplicate player 생성 방지
- [ ] existing duplicate player cleanup 완료
- [ ] existing duplicate match cleanup 완료
- [ ] `PL 2024-2025` sample UI 검증 완료
- [ ] `preferredArtifactSources` 실제 샘플 확인
- [ ] `BL1` 또는 `PD` 한 리그 추가 pilot 성공

## 결론

다음 rollout의 blocker는 더 이상 source 수집이 아니라 canonical cleanup이다.

즉 순서는:

```text
cleanup -> rerun -> verify -> next competition rollout
```
