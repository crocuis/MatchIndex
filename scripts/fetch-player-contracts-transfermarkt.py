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

POSITION_PATTERN = re.compile(
    r"➤\s*(?P<team>.+?),\s+since\s+\d{4}\s+➤\s*(?P<position>.+?)\s+➤\s*Market value:",
    flags=re.IGNORECASE,
)

DESCRIPTION_PATTERN = re.compile(
    r"^(?P<name>.+?),\s*(?P<age>\d+),\s*from\s+(?P<country>.+?)\s+➤\s*(?P<tail>.+)$",
    flags=re.IGNORECASE,
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


def extract_meta_content(html: str, key: str, attr: str = "property") -> str | None:
    pattern = rf'<meta\s+{attr}="{re.escape(key)}"\s+content="([^"]*)"'
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


def parse_market_value_eur(value: str | None) -> int | None:
    if not value:
        return None
    normalized = value.strip().lower().replace(",", ".").replace(" ", "")
    match = re.search(r"€(?P<amount>\d+(?:\.\d+)?)(?P<suffix>[kmb])", normalized)
    if not match:
        digits = re.sub(r"[^0-9]", "", normalized)
        return int(digits) if digits else None

    amount = float(match.group("amount"))
    multiplier = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[match.group("suffix")]
    return round(amount * multiplier)


def parse_age(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\((\d{1,2})\)", value)
    if match:
        return int(match.group(1))

    normalized = value.strip()
    return int(normalized) if normalized.isdigit() else None


def parse_meta_profile(html: str) -> dict[str, Any]:
    description = extract_meta_content(html, "description", "name") or ""
    image_url = extract_meta_content(html, "og:image")
    keywords = extract_meta_content(html, "keywords", "name") or ""
    profile = {
        "age": None,
        "marketValue": None,
        "nationalities": [],
        "photoUrl": image_url,
        "position": None,
        "teamName": None,
        "description": description,
        "keywords": keywords,
    }

    description_match = DESCRIPTION_PATTERN.match(description)
    if description_match:
        country = description_match.group("country").strip()
        if country:
            profile["nationalities"] = [country]
        profile["age"] = int(description_match.group("age"))

    position_match = POSITION_PATTERN.search(description)
    if position_match:
        profile["teamName"] = position_match.group("team").strip()
        profile["position"] = position_match.group("position").strip()

    market_value_match = re.search(r"Market value:\s*([^➤]+)", description, flags=re.IGNORECASE)
    if market_value_match:
        profile["marketValue"] = parse_market_value_eur(market_value_match.group(1).strip())

    if not profile["nationalities"] and keywords:
        keyword_parts = [part.strip() for part in keywords.split(",") if part.strip()]
        if keyword_parts:
            profile["nationalities"] = [keyword_parts[-1]]

    return profile


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
    meta_profile = parse_meta_profile(html)
    date_of_birth_text = extract_label_value(html, "Date of birth/Age")
    height_text = extract_label_value(html, "Height")
    foot_text = extract_label_value(html, "Foot")
    joined_text = extract_label_value(html, "Joined")
    contract_expires_text = extract_label_value(html, "Contract expires")
    return {
        "playerSlug": target.get("playerSlug"),
        "playerName": target.get("playerName") or parse_name(html),
        "age": meta_profile["age"] or parse_age(date_of_birth_text),
        "dateOfBirth": parse_date(date_of_birth_text),
        "heightCm": parse_height_cm(height_text),
        "preferredFoot": parse_preferred_foot(foot_text),
        "contractStartDate": parse_date(joined_text),
        "contractEndDate": parse_date(contract_expires_text),
        "marketValue": meta_profile["marketValue"],
        "nationalities": meta_profile["nationalities"],
        "photoUrl": meta_profile["photoUrl"],
        "position": meta_profile["position"],
        "sourceUrl": source_url.strip(),
        "teamName": target.get("teamName") or meta_profile["teamName"],
        "raw": {
            "dateOfBirthText": date_of_birth_text,
            "description": meta_profile["description"],
            "footText": foot_text,
            "heightText": height_text,
            "keywords": meta_profile["keywords"],
            "joinedText": joined_text,
            "contractExpiresText": contract_expires_text,
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
