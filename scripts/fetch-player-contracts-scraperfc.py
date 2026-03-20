#!/usr/bin/env python3
import argparse
import json
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def fail(message: str) -> None:
    raise SystemExit(message)


def get_scraperfc_classes() -> tuple[Any, Any]:
    imported_capology = None
    imported_transfermarkt = None

    try:
        from ScraperFC import Capology as ImportedCapology, Transfermarkt as ImportedTransfermarkt  # type: ignore
        imported_capology = ImportedCapology
        imported_transfermarkt = ImportedTransfermarkt
    except ImportError as error:
        fail(
            "ScraperFC is not installed. Install it in your Python environment first, "
            "for example: pip install ScraperFC selenium pandas requests beautifulsoup4 cloudscraper\n"
            f"Original import error: {error}"
        )

    if imported_capology is None or imported_transfermarkt is None:
        fail("ScraperFC classes could not be initialized.")

    return imported_capology, imported_transfermarkt


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_output(payload: dict[str, Any], output_path: str | None) -> None:
    serialized = json.dumps(payload, ensure_ascii=True, indent=2)
    if output_path:
        Path(output_path).write_text(serialized + "\n", encoding="utf-8")
        return

    print(serialized)


def read_json(path_value: str | None) -> dict[str, Any] | None:
    if not path_value:
        return None

    return json.loads(Path(path_value).read_text(encoding="utf-8"))


def normalize_text(value: str | None) -> str:
    if not value:
        return ""

    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii").lower()
    tokens = []
    current = []
    for character in normalized:
        if character.isalnum():
            current.append(character)
        else:
            if current:
                tokens.append("".join(current))
                current = []

    if current:
        tokens.append("".join(current))

    return " ".join(tokens)


def tokens(value: str | None) -> list[str]:
    normalized = normalize_text(value)
    return [token for token in normalized.split(" ") if token]


def build_acronym(value: str | None) -> str:
    return "".join(token[0] for token in tokens(value) if token)


def names_match(expected: str | None, actual: str | None) -> bool:
    expected_tokens = tokens(expected)
    actual_tokens = tokens(actual)
    if not expected_tokens or not actual_tokens:
        return False

    expected_joined = " ".join(expected_tokens)
    actual_joined = " ".join(actual_tokens)
    if expected_joined == actual_joined:
        return True

    if len(expected_tokens) == len(actual_tokens):
        if all(
            expected_token == actual_token or (len(expected_token) == 1 and actual_token.startswith(expected_token))
            for expected_token, actual_token in zip(expected_tokens, actual_tokens)
        ):
            return True

    return False


def team_names_match(expected: str | None, actual: str | None) -> bool:
    expected_normalized = normalize_text(expected)
    actual_normalized = normalize_text(actual)
    if not expected_normalized or not actual_normalized:
        return False
    if expected_normalized == actual_normalized:
        return True
    if build_acronym(expected) == actual_normalized or build_acronym(actual) == expected_normalized:
        return True
    return False


def get_link_player_key(player_link: str) -> str:
    parts = player_link.rstrip("/").split("/")
    if len(parts) < 3:
        return ""
    try:
        slug_index = parts.index("profil") - 1
    except ValueError:
        return ""
    if slug_index < 0:
        return ""
    return normalize_text(parts[slug_index].replace("-", " "))


def load_target_index(path_value: str | None) -> dict[str, set[str]]:
    payload = read_json(path_value)
    if not payload:
        return {}

    players: set[str] = set()
    player_team_pairs: set[str] = set()
    source_urls: set[str] = set()
    targets = payload.get("targets") or []
    for target in targets:
        if not isinstance(target, dict):
            continue

        player_names = target.get("playerNames") or []
        team_names = target.get("teamNames") or []
        source_url = target.get("sourceUrl")
        normalized_players = [normalize_text(str(value)) for value in player_names if normalize_text(str(value))]
        normalized_teams = [normalize_text(str(value)) for value in team_names if normalize_text(str(value))]
        for player_name in normalized_players:
            players.add(player_name)
            if normalized_teams:
                for team_name in normalized_teams:
                    player_team_pairs.add(f"{player_name}::{team_name}")
            else:
                player_team_pairs.add(f"{player_name}::")
        if isinstance(source_url, str) and source_url.strip():
            source_urls.add(source_url.strip())

    return {
        "players": players,
        "player_team_pairs": player_team_pairs,
        "source_urls": source_urls,
    }


def should_include_row(row: dict[str, Any], target_index: dict[str, set[str]]) -> bool:
    if not target_index:
        return True

    source_url = str(row.get("sourceUrl") or "").strip()
    if source_url and source_url in target_index.get("source_urls", set()):
        return True

    raw_player_name = str(row.get("playerName") or "")
    raw_team_name = str(row.get("teamName") or "")
    player_name = normalize_text(raw_player_name)
    if not player_name:
        return False

    target_players = target_index.get("players", set())
    if not any(names_match(target_player, raw_player_name) for target_player in target_players):
        return False

    pairs = target_index.get("player_team_pairs", set())
    if not pairs:
        return True

    for pair in pairs:
        pair_player, _, pair_team = pair.partition("::")
        if not names_match(pair_player, raw_player_name):
            continue
        if not pair_team:
            return True
        if team_names_match(pair_team, raw_team_name):
            return True

    return False


def flatten_columns(columns: list[Any]) -> list[str]:
    flattened: list[str] = []
    for col in columns:
        if isinstance(col, tuple):
            parts = [str(part).strip() for part in col if str(part).strip() and str(part).strip().lower() != "nan"]
            flattened.append(" ".join(parts))
        else:
            flattened.append(str(col).strip())
    return flattened


def normalize_key(value: str) -> str:
    return " ".join(value.lower().replace("_", " ").split())


def find_column(columns: list[str], predicates: list[tuple[str, ...]]) -> str | None:
    normalized = {column: normalize_key(column) for column in columns}
    for parts in predicates:
      for column, key in normalized.items():
          if all(part in key for part in parts):
              return column
    return None


def to_scalar(value: Any) -> Any:
    if value is None:
        return None
    try:
        import pandas as pd  # type: ignore

        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (str, int, float, bool)):
        return value

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)

    return str(value)


def scrape_capology_league(args: argparse.Namespace) -> dict[str, Any]:
    imported_capology, _ = get_scraperfc_classes()
    scraper = imported_capology()
    target_index = load_target_index(getattr(args, "targets", None))
    dataframe = scraper.scrape_salaries(args.season, args.league, args.currency.lower())
    dataframe.columns = flatten_columns(list(dataframe.columns))

    player_col = find_column(list(dataframe.columns), [("player",), ("name",)])
    team_col = find_column(list(dataframe.columns), [("team",), ("club",)])
    annual_col = find_column(list(dataframe.columns), [
        ("annual", "salary"),
        ("annual", "wage"),
        ("gross", "p/y"),
        ("gross", "per year"),
    ])
    weekly_col = find_column(list(dataframe.columns), [
        ("weekly", "salary"),
        ("weekly", "wage"),
        ("gross", "p/w"),
        ("gross", "per week"),
    ])

    if not player_col:
        fail(f"Unable to identify player column from Capology output columns: {list(dataframe.columns)}")

    rows: list[dict[str, Any]] = []
    source_url = None
    try:
        source_url = scraper.get_season_url(args.season, args.league)
    except Exception as error:
        try:
            source_url = scraper.get_league_url(args.league)
        except Exception:
            source_url = None
        print(f"Warning: failed to resolve Capology season URL for {args.league} {args.season}: {error}", file=sys.stderr)

    for _, record in dataframe.iterrows():
        player_name = to_scalar(record.get(player_col))
        if not player_name:
            continue

        row = {
            "playerName": str(player_name),
            "teamName": str(to_scalar(record.get(team_col))) if team_col and to_scalar(record.get(team_col)) else None,
            "annualSalary": to_scalar(record.get(annual_col)) if annual_col else None,
            "weeklyWage": to_scalar(record.get(weekly_col)) if weekly_col else None,
            "currencyCode": args.currency.upper(),
            "sourceUrl": source_url,
            "raw": {str(column): to_scalar(record.get(column)) for column in dataframe.columns},
        }

        if should_include_row(row, target_index):
            rows.append(row)

    return {
        "provider": "capology",
        "competition": args.league,
        "season": args.season,
        "fetchedAt": now_iso(),
        "sourceUrl": source_url,
        "currencyCode": args.currency.upper(),
        "rows": rows,
    }


def scrape_transfermarkt_player_link(scraper: Any, player_link: str) -> dict[str, Any]:
    try:
        dataframe = scraper.scrape_player(player_link)
    except Exception:
        return {
            "playerName": None,
            "teamName": None,
            "contractStartDate": None,
            "contractEndDate": None,
            "sourceUrl": player_link,
            "raw": {},
        }

    if dataframe.empty:
        return {
            "playerName": None,
            "teamName": None,
            "contractStartDate": None,
            "contractEndDate": None,
            "sourceUrl": player_link,
            "raw": {},
        }

    record = dataframe.iloc[0]
    return {
        "playerName": to_scalar(record.get("Name")),
        "teamName": to_scalar(record.get("Team")),
        "dateOfBirth": to_scalar(record.get("DOB")),
        "heightCm": round(float(to_scalar(record.get("Height (m)"))) * 100) if to_scalar(record.get("Height (m)")) not in (None, "") else None,
        "contractStartDate": to_scalar(record.get("Joined")) or to_scalar(record.get("Since")),
        "contractEndDate": to_scalar(record.get("Contract expiration")),
        "sourceUrl": player_link,
        "raw": {str(column): to_scalar(record.get(column)) for column in dataframe.columns},
    }


def scrape_transfermarkt_league(args: argparse.Namespace) -> dict[str, Any]:
    _, imported_transfermarkt = get_scraperfc_classes()
    scraper = imported_transfermarkt()
    target_index = load_target_index(getattr(args, "targets", None))
    prefiltered_by_player_link = False
    player_links = [
        player_link
        for player_link in scraper.get_player_links(args.season, args.league)
        if "/profil/spieler/" in player_link
    ]
    source_urls = target_index.get("source_urls", set())
    if source_urls:
        filtered_links = [player_link for player_link in player_links if player_link in source_urls]
        if filtered_links:
            player_links = filtered_links
            prefiltered_by_player_link = True
    elif target_index.get("players"):
        target_players = target_index.get("players", set())
        filtered_links = [
            player_link
            for player_link in player_links
            if any(names_match(target_player, get_link_player_key(player_link)) for target_player in target_players)
        ]
        if filtered_links:
            player_links = filtered_links
            prefiltered_by_player_link = True

    rows: list[dict[str, Any]] = []
    for player_link in player_links:
        row = scrape_transfermarkt_player_link(scraper, player_link)
        if prefiltered_by_player_link:
            if row.get("playerName"):
                rows.append(row)
            continue

        if should_include_row(row, target_index):
            rows.append(row)

    if args.limit:
        rows = rows[: args.limit]
    return {
        "provider": "transfermarkt",
        "competition": args.league,
        "season": args.season,
        "fetchedAt": now_iso(),
        "rows": rows,
    }


def scrape_transfermarkt_single_player(args: argparse.Namespace) -> dict[str, Any]:
    _, imported_transfermarkt = get_scraperfc_classes()
    scraper = imported_transfermarkt()
    row = scrape_transfermarkt_player_link(scraper, args.url)
    return {
        "provider": "transfermarkt",
        "fetchedAt": now_iso(),
        "rows": [row],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch player contract/salary data using ScraperFC")
    subparsers = parser.add_subparsers(dest="command", required=True)

    capology = subparsers.add_parser("capology-league", help="Fetch a league salary table from Capology")
    capology.add_argument("--league", required=True, help="ScraperFC Capology league name, e.g. 'England Premier League'")
    capology.add_argument("--season", required=True, help="ScraperFC season string, e.g. '2025-26'")
    capology.add_argument("--currency", default="eur", help="eur, gbp, or usd")
    capology.add_argument("--targets", help="Optional DB-exported target JSON used to filter rows")
    capology.add_argument("--output", help="Write normalized JSON to a file")

    tm_league = subparsers.add_parser("transfermarkt-league", help="Fetch league player contract dates from Transfermarkt")
    tm_league.add_argument("--league", required=True, help="ScraperFC Transfermarkt league name")
    tm_league.add_argument("--season", required=True, help="ScraperFC season string")
    tm_league.add_argument("--limit", type=int, help="Limit processed player links")
    tm_league.add_argument("--targets", help="Optional DB-exported target JSON used to filter rows or source URLs")
    tm_league.add_argument("--output", help="Write normalized JSON to a file")

    tm_player = subparsers.add_parser("transfermarkt-player", help="Fetch a single player profile from Transfermarkt")
    tm_player.add_argument("--url", required=True, help="Full Transfermarkt player URL")
    tm_player.add_argument("--output", help="Write normalized JSON to a file")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    payload: dict[str, Any] | None = None

    if args.command == "capology-league":
        payload = scrape_capology_league(args)
    elif args.command == "transfermarkt-league":
        payload = scrape_transfermarkt_league(args)
    elif args.command == "transfermarkt-player":
        payload = scrape_transfermarkt_single_player(args)
    else:
        fail(f"Unsupported command: {args.command}")

    if payload is None:
        fail("No payload produced.")

    write_output(payload, getattr(args, "output", None))  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
