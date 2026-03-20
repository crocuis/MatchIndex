#!/usr/bin/env python3
import argparse
import importlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SOFASCORE_API = "https://api.sofascore.com/api/v1/"

LEAGUE_BY_COMPETITION = {
    "PL": {
        "competitionName": "Premier League",
        "competitionSlug": "premier-league",
        "league": "ENG-Premier League",
    },
    "PD": {
        "competitionName": "La Liga",
        "competitionSlug": "la-liga",
        "league": "ESP-La Liga",
    },
    "SA": {
        "competitionName": "Serie A",
        "competitionSlug": "serie-a",
        "league": "ITA-Serie A",
    },
    "BL1": {
        "competitionName": "Bundesliga",
        "competitionSlug": "bundesliga",
        "league": "GER-Bundesliga",
    },
    "FL1": {
        "competitionName": "Ligue 1",
        "competitionSlug": "ligue-1",
        "league": "FRA-Ligue 1",
    },
    "UEL": {
        "competitionName": "UEFA Europa League",
        "competitionSlug": "uefa-europa-league",
        "league": "INT-UEFA Europa League",
        "sofascoreName": "UEFA Europa League",
        "uniqueTournamentId": 679,
    },
    "UCL": {
        "competitionName": "UEFA Champions League",
        "competitionSlug": "uefa-champions-league",
        "league": "INT-UEFA Champions League",
        "sofascoreName": "UEFA Champions League",
        "uniqueTournamentId": 7,
    },
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect Sofascore historical data through soccerdata")
    parser.add_argument("--competition", required=True, help="Competition code, for example PL")
    parser.add_argument("--season", required=True, help="Completed season label or start year")
    parser.add_argument("--output", help="Write collected JSONL payloads to a file")
    parser.add_argument("--data-dir", help="Optional soccerdata cache directory")
    parser.add_argument("--proxy", help="Optional soccerdata proxy value or 'tor'")
    parser.add_argument("--no-cache", action="store_true", help="Bypass existing soccerdata cache")
    parser.add_argument("--no-store", action="store_true", help="Do not persist downloaded soccerdata cache")
    parser.add_argument("--write", action="store_true", help="Collect data and emit JSONL output")
    parser.add_argument("--timeout", type=int, default=30, help="Per-request HTTP timeout in seconds (default: 30)")
    parser.add_argument("--skip-details", action="store_true", help="Skip per-match detail fetching (match_overview, match_lineups, match_events) for faster schedule-only runs")
    parser.add_argument("--max-matches", type=int, default=0, help="Stop after collecting this many match records (0 = unlimited)")
    parser.add_argument("--retry", type=int, default=0, help="Number of retry attempts on transient network errors (default: 0)")
    return parser


def normalize_season(value: str) -> int | str:
    stripped = value.strip()
    return int(stripped) if stripped.isdigit() else stripped


def serialize_records(frame: Any) -> list[dict[str, Any]]:
    normalized = frame.reset_index() if hasattr(frame, "reset_index") else frame
    serialized = normalized.to_json(orient="records", date_format="iso")
    data = json.loads(serialized)
    return data if isinstance(data, list) else []


def require_soccerdata() -> Any:
    try:
        soccerdata_module = importlib.import_module("soccerdata")
    except ModuleNotFoundError as error:
        raise SystemExit(
            "soccerdata is not installed. Install it in the selected Python environment before running this collector."
        ) from error

    return getattr(soccerdata_module, "Sofascore")


def require_tls_requests() -> Any:
    try:
        return importlib.import_module("tls_requests")
    except ModuleNotFoundError as error:
        raise SystemExit("tls_requests is not installed in the selected Python environment.") from error


def build_endpoint(league: str, season: str, dataset: str) -> str:
    return f"sofascore://{league}/{season}/{dataset}"


def make_row(
    *,
    competition_code: str,
    dataset: str,
    entity_type: str,
    external_id: str | None,
    external_parent_id: str | None,
    fetched_at: str,
    league: str,
    manifest_type: str,
    payload: dict[str, Any],
    season: str,
) -> dict[str, Any]:
    endpoint = build_endpoint(league, season, dataset)
    return {
        "endpoint": endpoint,
        "entityType": entity_type,
        "externalId": external_id,
        "externalParentId": external_parent_id,
        "manifestType": manifest_type,
        "metadata": {
            "competitionCode": competition_code,
            "dataset": dataset,
            "league": league,
            "season": season,
            "sourceReader": "sofascore",
        },
        "payload": payload,
        "seasonContext": season,
        "sourceAvailableAt": fetched_at,
        "sourceUpdatedAt": None,
        "upstreamPath": endpoint,
    }


def build_direct_client(args: argparse.Namespace) -> Any:
    return require_tls_requests()


def direct_get_json(client: Any, path: str, *, timeout_seconds: int = 30) -> dict[str, Any]:
    response = client.get(
        f"{SOFASCORE_API}{path}",
        client_identifier="chrome_136",
        random_tls_extension_order=True,
        timeout_seconds=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def try_direct_get_json(client: Any, path: str, *, timeout_seconds: int = 30, retries: int = 0) -> dict[str, Any] | None:
    for attempt in range(retries + 1):
        try:
            return direct_get_json(client, path, timeout_seconds=timeout_seconds)
        except Exception as error:
            message = str(error).lower()
            if "404" in message or "no recent network activity" in message:
                return None
            if "timeout" in message:
                if attempt < retries:
                    time.sleep(1)
                    continue
                return None
            if attempt < retries:
                time.sleep(1)
                continue
            raise
    return None


def load_round_events(client: Any, unique_tournament_id: int, season_id: int, round_item: dict[str, Any], *, timeout_seconds: int = 30, retries: int = 0) -> dict[str, Any] | None:
    round_number = round_item.get("round")
    round_slug = round_item.get("slug")
    candidates: list[str] = []
    if round_slug:
        candidates.append(
            f"unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}/slug/{round_slug}"
        )
    else:
        candidates.append(
            f"unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}"
        )

    for path in candidates:
        payload = try_direct_get_json(client, path, timeout_seconds=timeout_seconds, retries=retries)
        if payload:
            return payload
    return None


def resolve_tournament(config: dict[str, Any], tournaments: list[dict[str, Any]]) -> dict[str, Any] | None:
    def normalize(value: Any) -> str:
        return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())

    expected_slug = normalize(config.get("competitionSlug"))
    expected_name = normalize(config.get("sofascoreName") or config.get("competitionName"))
    expected_id = config.get("uniqueTournamentId")

    return next(
        (
            item
            for item in tournaments
            if item.get("id") == expected_id
            or normalize(item.get("slug")) == expected_slug
            or normalize(item.get("name")) == expected_name
        ),
        None,
    )


def normalize_sofascore_year_label(value: str) -> str:
    stripped = value.strip()
    if "/" in stripped:
        parts = stripped.split("/")
        if len(parts) == 2 and len(parts[0]) == 4:
            return f"{parts[0][2:]}/{parts[1][2:]}"
        return stripped
    if "-" in stripped:
        parts = stripped.split("-")
        if len(parts) == 2 and len(parts[0]) == 4:
            return f"{parts[0][2:]}/{parts[1][2:]}"
        return stripped
    if stripped.isdigit() and len(stripped) == 4:
        next_year = str(int(stripped) + 1)[2:]
        return f"{stripped[2:]}/{next_year}"
    return stripped


def is_supported_event_status(event: dict[str, Any]) -> bool:
    status = event.get("status", {})
    status_code = status.get("code")
    status_type = str(status.get("type") or "").strip().lower()
    return status_code == 0 or status_type == "finished" or status_code == 100


def extract_score(score: dict[str, Any] | None) -> int | None:
    if not score:
        return None
    for key in ("display", "normaltime", "current"):
        value = score.get(key)
        if isinstance(value, int):
            return value
    return None


def collect_rows_direct(config: dict[str, Any], args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fetched_at = now_iso()
    competition_code = args.competition.upper()
    league = config["league"]
    season_label = str(args.season)
    target_year = normalize_sofascore_year_label(season_label)
    session = build_direct_client(args)

    timeout_s: int = getattr(args, "timeout", 30)
    retry_n: int = getattr(args, "retry", 0)
    skip_details: bool = getattr(args, "skip_details", False)
    max_matches: int = getattr(args, "max_matches", 0)

    leagues_payload = direct_get_json(session, "config/default-unique-tournaments/EN/football", timeout_seconds=timeout_s)
    tournaments = leagues_payload.get("uniqueTournaments", [])
    tournament = resolve_tournament(config, tournaments)
    if not tournament:
        raise SystemExit(f"Sofascore tournament not found for {config['competitionName']}")

    tournament_id = tournament.get("id")
    if tournament_id is None:
        raise SystemExit(f"Sofascore tournament id missing for {config['competitionName']}")

    seasons_payload = direct_get_json(session, f"unique-tournament/{tournament_id}/seasons", timeout_seconds=timeout_s)
    season = next((item for item in seasons_payload.get("seasons", []) if item.get("year") == target_year), None)
    if not season:
        raise SystemExit(f"Sofascore season not found for {config['competitionName']} {season_label}")

    standings_payload = direct_get_json(
        session,
        f"unique-tournament/{tournament_id}/season/{season['id']}/standings/total",
        timeout_seconds=timeout_s,
    )
    rounds_payload = direct_get_json(
        session,
        f"unique-tournament/{tournament_id}/season/{season['id']}/rounds",
        timeout_seconds=timeout_s,
    )

    rows: list[dict[str, Any]] = []
    dataset_counts: dict[str, int] = {
        "league_info": 1,
        "season_info": 1,
        "league_table": 0,
        "match_events": 0,
        "match_lineups": 0,
        "match_overview": 0,
        "schedule": 0,
    }

    rows.append(
        make_row(
            competition_code=competition_code,
            dataset="league_info",
            entity_type="competition",
            external_id=str(tournament.get("id") or config["competitionSlug"]),
            external_parent_id=competition_code,
            fetched_at=fetched_at,
            league=league,
            manifest_type="competition_batch",
            payload={
                "league": tournament.get("name") or config.get("sofascoreName") or config["competitionName"],
                "league_id": tournament.get("id"),
                "region": tournament.get("category", {}).get("name"),
                "slug": tournament.get("slug"),
            },
            season=season_label,
        )
    )
    rows.append(
        make_row(
            competition_code=competition_code,
            dataset="season_info",
            entity_type="competition",
            external_id=season_label,
            external_parent_id=competition_code,
            fetched_at=fetched_at,
            league=league,
            manifest_type="competition_season_batch",
            payload={
                "league": league,
                "league_id": tournament_id,
                "season": season_label,
                "season_id": season.get("id"),
                "year": season.get("year"),
            },
            season=season_label,
        )
    )

    for standing in standings_payload.get("standings", []):
        for item in standing.get("rows", []):
            record = {
                "league": league,
                "season": season_label,
                "team": item["team"]["name"],
                "MP": item.get("matches"),
                "W": item.get("wins"),
                "D": item.get("draws"),
                "L": item.get("losses"),
                "GF": item.get("scoresFor"),
                "GA": item.get("scoresAgainst"),
                "GD": (item.get("scoresFor") or 0) - (item.get("scoresAgainst") or 0),
                "Pts": item.get("points"),
                "groupName": standing.get("name"),
            }
            rows.append(
                make_row(
                    competition_code=competition_code,
                    dataset="league_table",
                    entity_type="team",
                    external_id=item["team"]["name"],
                    external_parent_id=competition_code,
                    fetched_at=fetched_at,
                    league=league,
                    manifest_type="team_season_batch",
                    payload=record,
                    season=season_label,
                )
            )
            dataset_counts["league_table"] += 1

    seen_match_ids: set[int] = set()
    seen_round_keys: set[tuple[int | None, str | None, str | None]] = set()
    match_limit_reached = False
    for round_item in rounds_payload.get("rounds", []):
        if match_limit_reached:
            break
        round_number = round_item.get("round")
        round_key = (round_number, round_item.get("slug"), round_item.get("prefix"))
        if round_key in seen_round_keys:
            continue
        seen_round_keys.add(round_key)

        matches_payload = load_round_events(
            session, tournament_id, season["id"], round_item,
            timeout_seconds=timeout_s, retries=retry_n,
        )
        if not matches_payload:
            continue

        for event in matches_payload.get("events", []):
            status_code = event.get("status", {}).get("code")
            if not is_supported_event_status(event):
                continue
            if event.get("id") in seen_match_ids:
                continue
            if event.get("id") is not None:
                seen_match_ids.add(event["id"])

            record = {
                "league": league,
                "season": season_label,
                "round": round_number,
                "week": event.get("roundInfo", {}).get("round"),
                "roundName": round_item.get("name"),
                "date": datetime.fromtimestamp(event["startTimestamp"], tz=timezone.utc).isoformat(),
                "home_team": event["homeTeam"]["name"],
                "away_team": event["awayTeam"]["name"],
                "home_score": extract_score(event.get("homeScore")),
                "away_score": extract_score(event.get("awayScore")),
                "game_id": event.get("id"),
                "slug": event.get("slug"),
            }
            rows.append(
                make_row(
                    competition_code=competition_code,
                    dataset="schedule",
                    entity_type="match",
                    external_id=str(event.get("id") or "") or None,
                    external_parent_id=competition_code,
                    fetched_at=fetched_at,
                    league=league,
                    manifest_type="match_batch",
                    payload=record,
                    season=season_label,
                )
            )
            dataset_counts["schedule"] += 1

            if max_matches and dataset_counts["schedule"] >= max_matches:
                match_limit_reached = True
                break

            if skip_details:
                continue

            event_id = event.get("id")
            if event_id is None:
                continue

            event_detail = try_direct_get_json(session, f"event/{event_id}", timeout_seconds=timeout_s, retries=retry_n)
            event_statistics = try_direct_get_json(session, f"event/{event_id}/statistics", timeout_seconds=timeout_s, retries=retry_n)
            if event_detail and event_statistics:
                rows.append(
                    make_row(
                        competition_code=competition_code,
                        dataset="match_overview",
                        entity_type="match",
                        external_id=str(event_id),
                        external_parent_id=competition_code,
                        fetched_at=fetched_at,
                        league=league,
                        manifest_type="match_detail_batch",
                        payload={
                            "event": event_detail.get("event"),
                            "statistics": event_statistics.get("statistics"),
                        },
                        season=season_label,
                    )
                )
                dataset_counts["match_overview"] += 1

            event_lineups = try_direct_get_json(session, f"event/{event_id}/lineups", timeout_seconds=timeout_s, retries=retry_n)
            if event_lineups:
                rows.append(
                    make_row(
                        competition_code=competition_code,
                        dataset="match_lineups",
                        entity_type="match",
                        external_id=str(event_id),
                        external_parent_id=competition_code,
                        fetched_at=fetched_at,
                        league=league,
                        manifest_type="match_detail_batch",
                        payload=event_lineups,
                        season=season_label,
                    )
                )
                dataset_counts["match_lineups"] += 1

            event_incidents = try_direct_get_json(session, f"event/{event_id}/incidents", timeout_seconds=timeout_s, retries=retry_n)
            if event_incidents:
                rows.append(
                    make_row(
                        competition_code=competition_code,
                        dataset="match_events",
                        entity_type="match",
                        external_id=str(event_id),
                        external_parent_id=competition_code,
                        fetched_at=fetched_at,
                        league=league,
                        manifest_type="match_detail_batch",
                        payload=event_incidents,
                        season=season_label,
                    )
                )
                dataset_counts["match_events"] += 1

    summary = {
        "collector": "soccerdata_sofascore",
        "competition": competition_code,
        "season": season_label,
        "dryRun": not args.write,
        "implemented": True,
        "league": league,
        "datasetCounts": dataset_counts,
        "missingDatasets": ["player_season_stats_standard"],
        "payloadCount": len(rows),
        "fetchedAt": fetched_at,
        "outputPath": args.output,
        "mode": "direct_api",
        "seasonId": season.get("id"),
        "uniqueTournamentId": tournament_id,
    }
    return rows, summary


def collect_rows(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    competition_code = args.competition.upper()
    config = LEAGUE_BY_COMPETITION.get(competition_code)
    if not config:
        supported = ", ".join(sorted(LEAGUE_BY_COMPETITION))
        raise SystemExit(f"Unsupported competition code '{competition_code}'. Supported: {supported}")

    try:
        return collect_rows_direct(config, args)
    except Exception:
        if config.get("uniqueTournamentId"):
            raise

    Sofascore = require_soccerdata()
    season_value = normalize_season(args.season)
    fetched_at = now_iso()
    league = config["league"]
    season_label = str(args.season)
    reader_kwargs: dict[str, Any] = {
        "leagues": league,
        "seasons": season_value,
        "no_cache": args.no_cache,
        "no_store": args.no_store,
    }
    if args.proxy:
        reader_kwargs["proxy"] = args.proxy
    if args.data_dir:
        reader_kwargs["data_dir"] = args.data_dir

    reader = Sofascore(**reader_kwargs)

    datasets = {
        "league_info": (reader.read_leagues(), "competition", "competition_batch"),
        "season_info": (reader.read_seasons(), "competition", "competition_season_batch"),
        "schedule": (reader.read_schedule(), "match", "match_batch"),
        "league_table": (reader.read_league_table(), "team", "team_season_batch"),
    }

    rows: list[dict[str, Any]] = []
    dataset_counts: dict[str, int] = {}

    for dataset, (frame, entity_type, manifest_type) in datasets.items():
        records = serialize_records(frame)
        dataset_counts[dataset] = len(records)
        for record in records:
            if entity_type == "match":
                external_id = str(record.get("game_id") or record.get("game") or "") or None
            elif entity_type == "team":
                external_id = str(record.get("team") or "") or None
            else:
                external_id = str(record.get("league_id") or record.get("league") or config["competitionSlug"])

            rows.append(
                make_row(
                    competition_code=competition_code,
                    dataset=dataset,
                    entity_type=entity_type,
                    external_id=external_id,
                    external_parent_id=competition_code,
                    fetched_at=fetched_at,
                    league=league,
                    manifest_type=manifest_type,
                    payload=record,
                    season=season_label,
                )
            )

    summary = {
        "collector": "soccerdata_sofascore",
        "competition": competition_code,
        "season": season_label,
        "dryRun": not args.write,
        "implemented": True,
        "league": league,
        "datasetCounts": dataset_counts,
        "missingDatasets": ["player_season_stats_standard"],
        "payloadCount": len(rows),
        "fetchedAt": fetched_at,
        "outputPath": args.output,
    }
    return rows, summary


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.write and not args.output:
        raise SystemExit("--output is required when --write is used.")

    if not args.write:
        competition_code = args.competition.upper()
        config = LEAGUE_BY_COMPETITION.get(competition_code)
        if not config:
            supported = ", ".join(sorted(LEAGUE_BY_COMPETITION))
            raise SystemExit(f"Unsupported competition code '{competition_code}'. Supported: {supported}")

        summary = {
            "collector": "soccerdata_sofascore",
            "competition": competition_code,
            "season": str(args.season),
            "dryRun": True,
            "implemented": True,
            "league": config["league"],
            "plannedDatasets": [
                "league_info",
                "season_info",
                "schedule",
                "league_table",
                "match_overview",
                "match_lineups",
                "match_events",
            ],
            "mode": "direct_api" if config.get("uniqueTournamentId") else "soccerdata_reader",
            "missingDatasets": ["player_season_stats_standard"],
            "outputPath": args.output,
            "nextStep": "Run with --write and --output to collect JSONL payloads.",
        }
        print(json.dumps(summary, ensure_ascii=True))
        return

    try:
        rows, summary = collect_rows(args)
    except Exception as error:
        failure = {
            "collector": "soccerdata_sofascore",
            "competition": args.competition.upper(),
            "season": str(args.season),
            "dryRun": False,
            "implemented": True,
            "error": str(error),
            "hint": "Sofascore collector supports competition/season/schedule/league table. Player season stats are not available in soccerdata Sofascore.",
        }
        print(json.dumps(failure, ensure_ascii=True))
        raise SystemExit(1) from error

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(json.dumps(row, ensure_ascii=True) for row in rows) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True))


if __name__ == "__main__":
    main()
