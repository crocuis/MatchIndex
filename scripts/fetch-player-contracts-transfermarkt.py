#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime, timezone
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


def extract_label_value(html: str, label: str) -> str | None:
    pattern = rf'{re.escape(label)}:\s*<span[^>]*class="data-header__content"[^>]*>\s*([^<]+?)\s*</span>'
    match = re.search(pattern, html, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().split("(", 1)[0].strip()
    match = re.match(r"^(\d{2})\/(\d{2})\/(\d{4})$", cleaned)
    if not match:
        return None
    return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"


def parse_height_cm(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"(\d+),(\d+)\s*m", value)
    if not match:
        return None
    meters = float(f"{match.group(1)}.{match.group(2)}")
    return round(meters * 100)


def parse_preferred_foot(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized.startswith("left"):
        return "Left"
    if normalized.startswith("right"):
        return "Right"
    if normalized.startswith("both"):
        return "Both"
    return None


def parse_name(html: str) -> str | None:
    match = re.search(r'<meta property="og:title" content="([^"]+)"', html, flags=re.IGNORECASE)
    if not match:
      return None
    return match.group(1).split(" - ", 1)[0].strip()


def parse_target_row(target: dict[str, Any]) -> dict[str, Any] | None:
    source_url = target.get("sourceUrl")
    if not isinstance(source_url, str) or not source_url.strip():
        return None

    html = fetch_html(source_url.strip())
    return {
        "playerSlug": target.get("playerSlug"),
        "playerName": target.get("playerName") or parse_name(html),
        "dateOfBirth": parse_date(extract_label_value(html, "Date of birth/Age")),
        "heightCm": parse_height_cm(extract_label_value(html, "Height")),
        "preferredFoot": parse_preferred_foot(extract_label_value(html, "Foot")),
        "contractStartDate": parse_date(extract_label_value(html, "Joined")),
        "contractEndDate": parse_date(extract_label_value(html, "Contract expires")),
        "sourceUrl": source_url.strip(),
        "raw": {
            "dateOfBirthText": extract_label_value(html, "Date of birth/Age"),
            "heightText": extract_label_value(html, "Height"),
            "footText": extract_label_value(html, "Foot"),
            "joinedText": extract_label_value(html, "Joined"),
            "contractExpiresText": extract_label_value(html, "Contract expires"),
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch player contract/profile data from Transfermarkt")
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
        "provider": "transfermarkt",
        "competition": targets_payload.get("competitionSlug"),
        "season": targets_payload.get("seasonSlug"),
        "fetchedAt": now_iso(),
        "rows": rows,
    }
    write_output(payload, args.output)


if __name__ == "__main__":
    main()
