#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup, Comment
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

FBREF_BASE_URL = "https://fbref.com"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"


@dataclass(frozen=True)
class LeagueConfig:
    competition_id: str
    competition_name: str
    competition_slug: str
    seasons_slug: str
    league: str


LEAGUE_BY_COMPETITION: dict[str, LeagueConfig] = {
    "BL1": LeagueConfig("20", "Bundesliga", "bundesliga", "Bundesliga-Seasons", "GER-Bundesliga"),
    "FL1": LeagueConfig("13", "Ligue 1", "ligue-1", "Ligue-1-Seasons", "FRA-Ligue 1"),
    "PD": LeagueConfig("12", "La Liga", "la-liga", "La-Liga-Seasons", "ESP-La Liga"),
    "PL": LeagueConfig("9", "Premier League", "premier-league", "Premier-League-Seasons", "ENG-Premier League"),
    "SA": LeagueConfig("11", "Serie A", "serie-a", "Serie-A-Seasons", "ITA-Serie A"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect FBref season data through Selenium + real Chrome")
    parser.add_argument("--competition", required=True)
    parser.add_argument("--season", required=True)
    parser.add_argument("--cookie-file")
    parser.add_argument("--output")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--user-data-dir")
    parser.add_argument("--chrome-binary")
    parser.add_argument("--stay-open-seconds", type=int, default=0)
    return parser.parse_args()


def normalize_season_value(value: str) -> str:
    trimmed = value.strip()
    return f"{trimmed}-{int(trimmed) + 1}" if trimmed.isdigit() and len(trimmed) == 4 else trimmed


def parse_cookie_header(cookie_header: str) -> list[dict[str, str]]:
    cookies: list[dict[str, str]] = []
    for entry in cookie_header.split(";"):
        if "=" not in entry:
            continue
        name, value = entry.strip().split("=", 1)
        cookies.append({"domain": ".fbref.com", "name": name, "path": "/", "value": value})
    return cookies


def build_endpoint(league: str, season: str, dataset: str) -> str:
    return f"fbref-selenium://{league}/{season}/{dataset}"


def make_row(
    *,
    competition_code: str,
    dataset: str,
    entity_type: str,
    external_id: Optional[str],
    external_parent_id: Optional[str],
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


def random_delay(min_ms: int, max_ms: int) -> None:
    time.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


def create_driver(args: argparse.Namespace) -> webdriver.Chrome:
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-infobars")
    options.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if args.user_data_dir:
        options.add_argument(f"--user-data-dir={Path(args.user_data_dir).resolve()}")
    if args.chrome_binary:
        options.binary_location = args.chrome_binary
    if not args.headed:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        "Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});"
        "Object.defineProperty(navigator, 'language', {get: () => 'en-US'});"
        "Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});"
    )
    return driver


def add_cookies(driver: webdriver.Chrome, cookie_file: str | None) -> None:
    if not cookie_file:
        return
    cookie_header = Path(cookie_file).read_text(encoding="utf-8").strip()
    if not cookie_header:
        return
    driver.get(f"{FBREF_BASE_URL}/")
    for cookie in parse_cookie_header(cookie_header):
      driver.add_cookie(cookie)


def wait_for_fbref_ready(driver: webdriver.Chrome, url: str) -> None:
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            random_delay(700, 1800)
            driver.get(url)
            random_delay(1200, 2600)
            for _ in range(45):
                title = driver.title
                body_text = driver.find_element(By.TAG_NAME, "body").text
                if title and title != "Just a moment..." and "Checking your browser" not in body_text:
                    return
                random_delay(800, 1500)
            raise RuntimeError(f"FBref challenge did not clear for {url}")
        except Exception as error:  # noqa: PERF203
            last_error = error
            if attempt < 2:
                random_delay(1500 * (2**attempt), 2000 * (2**attempt))
    if last_error:
        raise last_error
    raise RuntimeError(f"FBref challenge did not clear for {url}")


def extract_rows_from_table_html(table_html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(table_html, "html.parser")
    rows: list[dict[str, str]] = []
    for row in soup.select("tbody tr"):
        row_node: Any = row
        classes = row_node.get("class") or []
        if "thead" in classes:
            continue
        record: dict[str, str] = {}
        for cell in row.select("th[data-stat], td[data-stat]"):
            cell_node: Any = cell
            key = cell_node.get("data-stat")
            if not key:
                continue
            record[str(key)] = cell_node.get_text(strip=True)
            anchor: Any = cell_node.find("a")
            href = anchor.get("href") if anchor else None
            if href:
                record[f"{key}_href"] = str(href)
        rows.append(record)
    return rows


def extract_table_rows(driver: webdriver.Chrome, selector: str) -> list[dict[str, str]]:
    table = driver.find_element(By.CSS_SELECTOR, selector)
    html = table.get_attribute("outerHTML")
    if not html:
        raise RuntimeError(f"Table not found for selector: {selector}")
    return extract_rows_from_table_html(html)


def extract_comment_table_rows(driver: webdriver.Chrome, table_id: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment_text = str(comment)
        if table_id not in comment_text:
            continue
        comment_soup = BeautifulSoup(comment_text, "html.parser")
        table = comment_soup.select_one(f"table#{table_id}")
        if table:
            return extract_rows_from_table_html(str(table))
    raise RuntimeError(f"Comment table not found for id: {table_id}")


def read_league_row(driver: webdriver.Chrome, competition_name: str) -> dict[str, Optional[str]]:
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for table in soup.select("table[id*='comps']"):
        for row in table.select("tbody tr"):
            cell: Any = row.select_one("th[data-stat='league_name'], td[data-stat='league_name']")
            text = cell.get_text(strip=True) if cell else ""
            anchor: Any = cell.find("a") if cell else None
            href = anchor.get("href") if anchor else None
            if text == competition_name and href:
                first_cell: Any = row.select_one("td[data-stat='first_season']")
                last_cell: Any = row.select_one("td[data-stat='last_season']")
                return {
                    "firstSeason": first_cell.get_text(strip=True) if first_cell else None,
                    "lastSeason": last_cell.get_text(strip=True) if last_cell else None,
                    "league": text,
                    "url": str(href),
                }
    raise RuntimeError(f"League row not found for {competition_name}")


def build_history_url(config: LeagueConfig) -> str:
    return f"{FBREF_BASE_URL}/en/comps/{config.competition_id}/history/{config.seasons_slug}"


def read_season_row(driver: webdriver.Chrome, season_input: str) -> dict[str, Optional[str]]:
    normalized = normalize_season_value(season_input)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.select_one("table#seasons")
    if not table:
        raise RuntimeError("Seasons table not found")
    for row in table.select("tbody tr"):
        cell: Any = row.select_one("th[data-stat='year_id'], th[data-stat='year'], td[data-stat='year_id'], td[data-stat='year']")
        season_label = cell.get_text(strip=True) if cell else ""
        anchor: Any = cell.find("a") if cell else None
        href = anchor.get("href") if anchor else None
        if href and (season_label == normalized or season_label.startswith(normalized)):
            return {
                "format": "elimination" if row.select_one("td[data-stat='final']") else "round-robin",
                "seasonLabel": season_label,
                "url": str(href),
            }
    raise RuntimeError(f"Season row not found for {season_input}")


def read_schedule_path(driver: webdriver.Chrome) -> str:
    links = driver.find_elements(By.TAG_NAME, "a")
    for link in links:
        if (link.text or "").strip() == "Scores & Fixtures":
            href = link.get_attribute("href")
            if href:
                return href.replace(FBREF_BASE_URL, "")
    raise RuntimeError("Scores & Fixtures link not found")


def derive_player_stats_path(season_path: str) -> str:
    parts = season_path.split("/")
    slug = parts[-1]
    if not slug:
        raise RuntimeError(f"Invalid season path: {season_path}")
    return f"{'/'.join(parts[:-1])}/stats/{slug}"


def collect_rows(args: argparse.Namespace) -> dict[str, Any]:
    config = LEAGUE_BY_COMPETITION.get(args.competition)
    if not config:
        raise RuntimeError(f"Unsupported competition code '{args.competition}'")

    driver = create_driver(args)
    fetched_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows: list[dict[str, Any]] = []
    dataset_counts: dict[str, int] = {}
    try:
        add_cookies(driver, args.cookie_file)
        wait_for_fbref_ready(driver, build_history_url(config))
        league_row = {
            "firstSeason": None,
            "lastSeason": None,
            "league": config.competition_name,
            "url": f"/en/comps/{config.competition_id}/history/{config.seasons_slug}",
        }
        rows.append(make_row(competition_code=args.competition, dataset="league_info", entity_type="competition", external_id=config.competition_slug, external_parent_id=args.competition, fetched_at=fetched_at, league=config.league, manifest_type="competition_batch", payload=league_row, season=args.season))
        dataset_counts["league_info"] = 1

        wait_for_fbref_ready(driver, f"{FBREF_BASE_URL}{league_row['url']}")
        season_row = read_season_row(driver, args.season)
        rows.append(make_row(competition_code=args.competition, dataset="season_info", entity_type="competition", external_id=args.season, external_parent_id=args.competition, fetched_at=fetched_at, league=config.league, manifest_type="competition_season_batch", payload=season_row, season=args.season))
        dataset_counts["season_info"] = 1

        wait_for_fbref_ready(driver, f"{FBREF_BASE_URL}{season_row['url']}")
        team_rows = extract_table_rows(driver, "table#stats_squads_standard_for")
        dataset_counts["team_season_stats_standard"] = len(team_rows)
        for record in team_rows:
            rows.append(make_row(competition_code=args.competition, dataset="team_season_stats_standard", entity_type="team", external_id=record.get("squad"), external_parent_id=args.competition, fetched_at=fetched_at, league=config.league, manifest_type="team_season_batch", payload=record, season=args.season))

        schedule_path = read_schedule_path(driver)
        wait_for_fbref_ready(driver, f"{FBREF_BASE_URL}{schedule_path}")
        schedule_rows = extract_table_rows(driver, "table[id*='sched']")
        dataset_counts["schedule"] = len(schedule_rows)
        for record in schedule_rows:
            href = record.get("match_report_href") or ""
            game_id = href.split("/")[3] if href and len(href.split("/")) > 3 else record.get("date")
            rows.append(make_row(competition_code=args.competition, dataset="schedule", entity_type="match", external_id=game_id, external_parent_id=args.competition, fetched_at=fetched_at, league=config.league, manifest_type="match_batch", payload=record, season=args.season))

        player_stats_path = derive_player_stats_path(str(season_row["url"]))
        wait_for_fbref_ready(driver, f"{FBREF_BASE_URL}{player_stats_path}")
        player_rows = extract_comment_table_rows(driver, "stats_standard")
        dataset_counts["player_season_stats_standard"] = len(player_rows)
        for record in player_rows:
            if record.get("player") == "Player":
                continue
            player_name = (record.get("player") or "").strip()
            team_name = (record.get("squad") or "").strip()
            external_id = f"{player_name}|{team_name}|{args.season}" if player_name else None
            rows.append(make_row(competition_code=args.competition, dataset="player_season_stats_standard", entity_type="player", external_id=external_id, external_parent_id=args.competition, fetched_at=fetched_at, league=config.league, manifest_type="player_season_batch", payload=record, season=args.season))

        if args.headed and args.stay_open_seconds > 0:
            time.sleep(args.stay_open_seconds)

        return {
            "datasetCounts": dataset_counts,
            "fetchedAt": fetched_at,
            "league": config.league,
            "payloadCount": len(rows),
            "rows": rows,
        }
    finally:
        driver.quit()


def main() -> None:
    args = parse_args()
    config = LEAGUE_BY_COMPETITION.get(args.competition)
    if not config:
      raise RuntimeError(f"Unsupported competition code '{args.competition}'")

    if not args.write:
        print(json.dumps({
            "collector": "fbref_selenium",
            "competition": args.competition,
            "season": args.season,
            "dryRun": True,
            "headed": args.headed,
            "implemented": True,
            "league": config.league,
            "cookieFileConfigured": bool(args.cookie_file),
            "persistentProfileDir": args.user_data_dir,
            "plannedDatasets": ["league_info", "season_info", "schedule", "team_season_stats_standard", "player_season_stats_standard"],
            "outputPath": args.output,
            "nextStep": "Run with --write and --output. Prefer --headed with --user-data-dir for Cloudflare-heavy flows.",
        }))
        return

    if not args.output:
        raise RuntimeError("--output is required when --write is used")

    try:
        result = collect_rows(args)
        output_path = Path(args.output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(json.dumps(row, ensure_ascii=True) for row in result["rows"]) + "\n", encoding="utf-8")
        print(json.dumps({
            "collector": "fbref_selenium",
            "competition": args.competition,
            "cookieFileConfigured": bool(args.cookie_file),
            "season": args.season,
            "dryRun": False,
            "headed": args.headed,
            "implemented": True,
            "persistentProfileDir": args.user_data_dir,
            "league": result["league"],
            "datasetCounts": result["datasetCounts"],
            "payloadCount": result["payloadCount"],
            "fetchedAt": result["fetchedAt"],
            "outputPath": args.output,
        }))
    except Exception as error:
        print(json.dumps({
            "collector": "fbref_selenium",
            "competition": args.competition,
            "cookieFileConfigured": bool(args.cookie_file),
            "season": args.season,
            "dryRun": False,
            "headed": args.headed,
            "implemented": True,
            "persistentProfileDir": args.user_data_dir,
            "error": str(error),
            "hint": "Selenium real-Chrome path failed. Ensure Chrome can launch, the user profile is valid, and challenge is manually cleared when needed.",
        }))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
