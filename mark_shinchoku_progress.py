#!/usr/bin/env python3
"""SQLite の回答データをもとに進捗スプレッドシートへ "済" を記録する。"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

DB_TABLE_NAME = "invite_answers"


def default_shinchoku_config_path() -> Path:
    preferred = Path("shinchoku.json")
    fallback = Path("shichoku.json")
    if preferred.exists():
        return preferred
    return fallback


def load_shinchoku_settings(path: Path) -> dict[str, object]:
    with path.open(encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict) or "shinchoku" not in raw or not isinstance(raw["shinchoku"], dict):
        raise ValueError("設定 JSON は {\"shinchoku\": {...}} 形式である必要があります。")

    settings = raw["shinchoku"]
    for required_key in ("spreadsheet_key", "sheet_name"):
        if not settings.get(required_key):
            raise ValueError(f"設定不足: shinchoku.{required_key}")

    return settings


def load_db_rows(db_path: Path) -> list[tuple[str, str]]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            f"""
            SELECT line_name, curriculum
            FROM {DB_TABLE_NAME}
            WHERE COALESCE(TRIM(line_name), '') <> ''
              AND COALESCE(TRIM(curriculum), '') <> ''
            """
        )
        rows = [(str(line_name).strip(), str(curriculum).strip()) for line_name, curriculum in cur.fetchall()]
        return rows
    finally:
        conn.close()


def normalize_curriculum_key(curriculum: str) -> str:
    key = curriculum.strip().lower().replace("-", "").replace("_", "")
    match = re.search(r"week\s*(\d+)", key)
    if not match:
        return ""
    return f"week{match.group(1)}_column"


def column_to_a1(column_index: int) -> str:
    if column_index < 1:
        raise ValueError(f"列番号は 1 以上である必要があります: {column_index}")

    result = []
    n = column_index
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result.append(chr(ord("A") + rem))
    return "".join(reversed(result))


def get_sheet_values(service, spreadsheet_id: str, sheet_name: str) -> list[list[str]]:
    response = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=sheet_name)
        .execute()
    )
    values = response.get("values", [])
    return values if isinstance(values, list) else []


def extract_cell(rows: list[list[str]], row_idx_1based: int, col_idx_1based: int) -> str:
    row_idx = row_idx_1based - 1
    col_idx = col_idx_1based - 1
    if row_idx < 0 or col_idx < 0:
        return ""
    if row_idx >= len(rows):
        return ""
    row = rows[row_idx]
    if col_idx >= len(row):
        return ""
    return str(row[col_idx]).strip()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DB の line_name/curriculum をもとに進捗シートへ \"済\" を記録します。"
    )
    parser.add_argument("--db", type=Path, default=Path("invite_answers.db"), help="入力 SQLite DB")
    parser.add_argument(
        "--config",
        type=Path,
        default=default_shinchoku_config_path(),
        help="進捗シート設定 JSON (既定: shinchoku.json or shichoku.json)",
    )
    parser.add_argument(
        "--service-account",
        type=Path,
        default=Path("service_account.json"),
        help="Google Service Account JSON",
    )
    parser.add_argument(
        "--line-name-column",
        type=int,
        default=5,
        help="進捗シートで line_name を検索する列番号 (1始まり)",
    )
    parser.add_argument(
        "--mark-value",
        default="済",
        help="更新時に入力する値 (既定: 済)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("shinchoku_update_report.json"),
        help="実行結果 JSON の保存先",
    )
    args = parser.parse_args()

    settings = load_shinchoku_settings(args.config)
    spreadsheet_id = str(settings["spreadsheet_key"])
    sheet_name = str(settings["sheet_name"])

    rows = load_db_rows(args.db)

    credentials = Credentials.from_service_account_file(
        str(args.service_account),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    service = build("sheets", "v4", credentials=credentials)

    sheet_values = get_sheet_values(service, spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

    line_name_to_rows: dict[str, list[int]] = defaultdict(list)
    for row_idx_1based, row in enumerate(sheet_values, start=1):
        if len(row) < args.line_name_column:
            continue
        line_name = str(row[args.line_name_column - 1]).strip()
        if not line_name:
            continue
        line_name_to_rows[line_name].append(row_idx_1based)

    report: dict[str, object] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "db": str(args.db),
        "config": str(args.config),
        "spreadsheet_key": spreadsheet_id,
        "sheet_name": sheet_name,
        "line_name_column": args.line_name_column,
        "mark_value": args.mark_value,
        "summary": {
            "db_rows": len(rows),
            "updated": 0,
            "skip_unmatched": 0,
            "skip_duplicate_in_sheet": 0,
            "skip_unknown_curriculum": 0,
            "skip_already_filled": 0,
        },
        "details": {
            "unmatched": [],
            "duplicate_in_sheet": [],
            "unknown_curriculum": [],
            "already_filled": [],
            "updated": [],
        },
    }

    updates: list[dict[str, str]] = []

    for line_name, curriculum in rows:
        week_column_key = normalize_curriculum_key(curriculum)
        week_column = settings.get(week_column_key)
        if not isinstance(week_column, int):
            report["summary"]["skip_unknown_curriculum"] += 1
            report["details"]["unknown_curriculum"].append(
                {
                    "line_name": line_name,
                    "curriculum": curriculum,
                    "expected_column_key": week_column_key,
                }
            )
            print(
                f"[SKIP][UNKNOWN_CURRICULUM] line_name={line_name} curriculum={curriculum} key={week_column_key}"
            )
            continue

        matched_rows = line_name_to_rows.get(line_name, [])
        if not matched_rows:
            report["summary"]["skip_unmatched"] += 1
            report["details"]["unmatched"].append(
                {
                    "line_name": line_name,
                    "curriculum": curriculum,
                }
            )
            print(f"[SKIP][UNMATCHED] line_name={line_name} curriculum={curriculum}")
            continue

        if len(matched_rows) > 1:
            report["summary"]["skip_duplicate_in_sheet"] += 1
            report["details"]["duplicate_in_sheet"].append(
                {
                    "line_name": line_name,
                    "curriculum": curriculum,
                    "matched_rows": matched_rows,
                }
            )
            print(
                f"[SKIP][DUPLICATE_IN_SHEET] line_name={line_name} curriculum={curriculum} rows={matched_rows}"
            )
            continue

        row_idx = matched_rows[0]
        current_value = extract_cell(sheet_values, row_idx_1based=row_idx, col_idx_1based=week_column)
        if current_value:
            report["summary"]["skip_already_filled"] += 1
            report["details"]["already_filled"].append(
                {
                    "line_name": line_name,
                    "curriculum": curriculum,
                    "row": row_idx,
                    "column": week_column,
                    "current_value": current_value,
                }
            )
            print(
                f"[SKIP][ALREADY_FILLED] line_name={line_name} curriculum={curriculum} row={row_idx} col={week_column} value={current_value}"
            )
            continue

        cell = f"{sheet_name}!{column_to_a1(week_column)}{row_idx}"
        updates.append({"range": cell, "values": [[args.mark_value]]})
        report["details"]["updated"].append(
            {
                "line_name": line_name,
                "curriculum": curriculum,
                "row": row_idx,
                "column": week_column,
                "a1": cell,
                "value": args.mark_value,
            }
        )

    if updates:
        (
            service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "valueInputOption": "RAW",
                    "data": updates,
                },
            )
            .execute()
        )

    report["summary"]["updated"] = len(updates)
    report["finished_at"] = datetime.now(timezone.utc).isoformat()

    with args.report.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(
        "完了: "
        f"updated={report['summary']['updated']} "
        f"skip_unmatched={report['summary']['skip_unmatched']} "
        f"skip_duplicate_in_sheet={report['summary']['skip_duplicate_in_sheet']} "
        f"skip_unknown_curriculum={report['summary']['skip_unknown_curriculum']} "
        f"skip_already_filled={report['summary']['skip_already_filled']} "
        f"report={args.report}"
    )


if __name__ == "__main__":
    main()
