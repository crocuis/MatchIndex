#!/usr/bin/env python3
import argparse
import importlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect FBref historical data through soccerdata")
    parser.add_argument("--competition", required=True, help="Competition code, for example PL")
    parser.add_argument("--season", required=True, help="Completed season label or start year")
    parser.add_argument("--output", help="Write collected JSONL payloads to a file")
    parser.add_argument("--cookie", help="Optional Cookie header value for Cloudflare-passed FBref session")
    parser.add_argument("--cookie-file", help="Read Cookie header value from a local text file")
    parser.add_argument("--data-dir", help="Optional soccerdata cache directory")
    parser.add_argument("--proxy", help="Optional soccerdata proxy value or 'tor'")
    parser.add_argument("--no-cache", action="store_true", help="Bypass existing soccerdata cache")
    parser.add_argument("--no-store", action="store_true", help="Do not persist downloaded soccerdata cache")
    parser.add_argument("--write", action="store_true", help="Collect data and emit JSONL output")
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

    return getattr(soccerdata_module, "FBref")


def resolve_cookie_header(args: argparse.Namespace) -> str | None:
    if args.cookie:
        return args.cookie.strip() or None

    cookie_file = args.cookie_file or os.environ.get("SOCCERDATA_COOKIE_FILE")
    if cookie_file:
        return Path(cookie_file).read_text(encoding="utf-8").strip() or None

    env_cookie = os.environ.get("SOCCERDATA_COOKIE")
    if env_cookie:
        return env_cookie.strip() or None

    return None


def apply_fbref_headers(cookie_header: str | None) -> None:
    fbref_module = importlib.import_module("soccerdata.fbref")
    headers = dict(getattr(fbref_module, "FBREF_HEADERS", {}))
    headers.setdefault(
        "user-agent",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    )
    headers.setdefault("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    headers.setdefault("accept-language", "en-US,en;q=0.9")
    headers.setdefault("cache-control", "no-cache")
    headers.setdefault("pragma", "no-cache")
    headers.setdefault("sec-fetch-dest", "document")
    headers.setdefault("sec-fetch-mode", "navigate")
    headers.setdefault("sec-fetch-site", "none")
    headers.setdefault("upgrade-insecure-requests", "1")
    if cookie_header:
        headers["cookie"] = cookie_header
    setattr(fbref_module, "FBREF_HEADERS", headers)


def build_endpoint(league: str, season: str, dataset: str) -> str:
    return f"fbref://{league}/{season}/{dataset}"


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
        },
        "payload": payload,
        "seasonContext": season,
        "sourceAvailableAt": fetched_at,
        "sourceUpdatedAt": None,
        "upstreamPath": endpoint,
    }


def collect_rows(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    competition_code = args.competition.upper()
    config = LEAGUE_BY_COMPETITION.get(competition_code)
    if not config:
        supported = ", ".join(sorted(LEAGUE_BY_COMPETITION))
        raise SystemExit(f"Unsupported competition code '{competition_code}'. Supported: {supported}")

    FBref = require_soccerdata()
    cookie_header = resolve_cookie_header(args)
    apply_fbref_headers(cookie_header)
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
    proxy = args.proxy or os.environ.get("SOCCERDATA_PROXY")
    if proxy:
        reader_kwargs["proxy"] = proxy
    if args.data_dir:
        reader_kwargs["data_dir"] = args.data_dir
    reader = FBref(**reader_kwargs)

    datasets = {
        "league_info": (reader.read_leagues(), "competition", "competition_batch"),
        "season_info": (reader.read_seasons(), "competition", "competition_season_batch"),
        "schedule": (reader.read_schedule(), "match", "match_batch"),
        "team_season_stats_standard": (reader.read_team_season_stats(stat_type="standard"), "team", "team_season_batch"),
        "player_season_stats_standard": (reader.read_player_season_stats(stat_type="standard"), "player", "player_season_batch"),
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
            elif entity_type == "player":
                player_name = str(record.get("player") or "").strip()
                team_name = str(record.get("team") or "").strip()
                external_id = f"{player_name}|{team_name}|{season_label}" if player_name else None
            else:
                external_id = str(record.get("league") or config["competitionSlug"] or competition_code)

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
        "collector": "soccerdata_fbref",
        "cookieConfigured": bool(cookie_header),
        "competition": competition_code,
        "season": season_label,
        "dryRun": not args.write,
        "implemented": True,
        "league": league,
        "datasetCounts": dataset_counts,
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
            "collector": "soccerdata_fbref",
            "cookieConfigured": bool(resolve_cookie_header(args)),
            "competition": competition_code,
            "season": str(args.season),
            "dryRun": True,
            "implemented": True,
            "league": config["league"],
            "plannedDatasets": [
                "league_info",
                "season_info",
                "schedule",
                "team_season_stats_standard",
                "player_season_stats_standard",
            ],
            "outputPath": args.output,
            "nextStep": "Run with --write and --output to collect JSONL payloads.",
        }
        print(json.dumps(summary, ensure_ascii=True))
        return

    try:
        rows, summary = collect_rows(args)
    except Exception as error:
        failure = {
            "collector": "soccerdata_fbref",
            "cookieConfigured": bool(resolve_cookie_header(args)),
            "competition": args.competition.upper(),
            "season": str(args.season),
            "dryRun": False,
            "implemented": True,
            "error": str(error),
            "hint": "Try Cloudflare-passed browser cookies via --cookie/--cookie-file and optionally combine with --proxy.",
        }
        print(json.dumps(failure, ensure_ascii=True))
        raise SystemExit(1) from error

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(json.dumps(row, ensure_ascii=True) for row in rows) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True))


if __name__ == "__main__":
    main()
