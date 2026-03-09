#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path_value: str) -> dict[str, Any]:
    return json.loads(Path(path_value).read_text(encoding="utf-8"))


def write_output(payload: dict[str, Any], output_path: str | None) -> None:
    serialized = json.dumps(payload, ensure_ascii=True, indent=2)
    if output_path:
        Path(output_path).write_text(serialized + "\n", encoding="utf-8")
        return
    print(serialized)


def fetch_html(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})
    with urlopen(request, timeout=30) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def strip_tags(html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_birth_date(html: str) -> str | None:
    match = re.search(r'id="necro-birth"[^>]*data-birth="(\d{4}-\d{2}-\d{2})"', html)
    return match.group(1) if match else None


def parse_height_cm(text: str) -> int | None:
    match = re.search(r"\b(\d{3})cm\b", text)
    return int(match.group(1)) if match else None


def parse_weight_kg(text: str) -> int | None:
    match = re.search(r"\b(\d{2,3})kg\b", text)
    return int(match.group(1)) if match else None


def normalize_foot(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized.startswith("left"):
        return "Left"
    if normalized.startswith("right"):
        return "Right"
    if normalized.startswith("both") or normalized.startswith("either"):
        return "Both"
    return None


def parse_preferred_foot(text: str) -> str | None:
    match = re.search(r"Footed:\s*([A-Za-z]+)", text)
    return normalize_foot(match.group(1) if match else None)


def parse_name(html: str) -> str | None:
    match = re.search(r"<h1[^>]*>([\s\S]*?)</h1>", html, flags=re.IGNORECASE)
    if not match:
        return None
    return strip_tags(match.group(1))


def parse_target_row(target: dict[str, Any]) -> dict[str, Any] | None:
    fbref_url = target.get("fbrefUrl")
    if not isinstance(fbref_url, str) or not fbref_url.strip():
        return None

    html = fetch_html(fbref_url.strip())
    page_text = strip_tags(html)
    height_match = re.search(r"\b\d{3}cm\b", page_text)
    weight_match = re.search(r"\b\d{2,3}kg\b", page_text)
    return {
        "playerSlug": target.get("playerSlug"),
        "playerName": target.get("playerName") or parse_name(html),
        "dateOfBirth": parse_birth_date(html),
        "heightCm": parse_height_cm(page_text),
        "weightKg": parse_weight_kg(page_text),
        "preferredFoot": parse_preferred_foot(page_text),
        "sourceUrl": fbref_url.strip(),
        "raw": {
            "birthDate": parse_birth_date(html),
            "heightText": height_match.group(0) if height_match else None,
            "weightText": weight_match.group(0) if weight_match else None,
            "preferredFoot": parse_preferred_foot(page_text),
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch player body profile data from FBref")
    parser.add_argument("--targets", required=True, help="JSON file produced by export-player-contract-targets.mts")
    parser.add_argument("--output", help="Write normalized JSON to a file")
    parser.add_argument("--limit", type=int, help="Limit processed targets")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    targets_payload = read_json(args.targets)
    targets = targets_payload.get("targets") or []
    if args.limit:
        targets = targets[: args.limit]

    rows = []
    for target in targets:
      if not isinstance(target, dict):
          continue
      row = parse_target_row(target)
      if row:
          rows.append(row)

    payload = {
        "provider": "fbref",
        "competition": targets_payload.get("competitionSlug"),
        "season": targets_payload.get("seasonSlug"),
        "fetchedAt": now_iso(),
        "rows": rows,
    }
    write_output(payload, args.output)


if __name__ == "__main__":
    main()
