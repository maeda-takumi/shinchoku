#!/usr/bin/env python3
"""notion_invite_targets.json をもとに Google スプレッドシートの値を SQLite に保存するスクリプト。"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import urllib.parse
import urllib.request
from pathlib import Path

TABLE_NAME = "invite_answers"


def fetch_sheet_rows(spreadsheet_key: str, sheet_name: str) -> list[list[str]]:
    encoded_sheet_name = urllib.parse.quote(sheet_name)
    url = (
        f"https://docs.google.com/spreadsheets/d/{spreadsheet_key}/gviz/tq"
        f"?tqx=out:csv&sheet={encoded_sheet_name}"
    )

    with urllib.request.urlopen(url) as response:
        decoded = response.read().decode("utf-8-sig")

    reader = csv.reader(decoded.splitlines())
    return list(reader)


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            line_name TEXT,
            curriculum TEXT NOT NULL,
            answer_date TEXT
        )
        """
    )

    # 再取り込み時に同一ユーザー行を更新できるよう、重複行を整理して一意制約を付与する。
    conn.execute(
        f"""
        DELETE FROM {TABLE_NAME}
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM {TABLE_NAME}
            GROUP BY curriculum, line_name
        )
        """
    )
    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_curriculum_line_name "
        f"ON {TABLE_NAME} (curriculum, line_name)"
    )

def upsert_rows(
    conn: sqlite3.Connection,
    curriculum: str,
    rows: list[list[str]],
    line_name_column: int,
    answer_date_column: int,
) -> int:
    upserted = 0
    # 1 行目はヘッダーのため取り込み対象外。
    for row in rows[1:]:
        line_name = row[line_name_column - 1].strip() if len(row) >= line_name_column else ""
        answer_date = row[answer_date_column - 1].strip() if len(row) >= answer_date_column else ""

        if not line_name and not answer_date:
            continue

        conn.execute(
            f"""
            INSERT INTO {TABLE_NAME} (line_name, curriculum, answer_date)
            VALUES (?, ?, ?)
            ON CONFLICT(curriculum, line_name)
            DO UPDATE SET answer_date = excluded.answer_date
            """,
            (line_name, curriculum, answer_date),
        )
        upserted += 1

    return upserted


def load_targets(path: Path) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("JSON のトップレベルはオブジェクト形式である必要があります。")

    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="notion_invite_targets.json に定義されたスプレッドシートから DB に値を取り込みます。"
    )
    parser.add_argument(
        "--targets",
        type=Path,
        default=Path("notion_invite_targets.json"),
        help="取り込み対象 JSON ファイル",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("invite_answers.db"),
        help="出力先 SQLite DB ファイル",
    )
    parser.add_argument(
        "--line-name-column",
        type=int,
        default=5,
        help="line_name を取得する列番号 (1始まり)",
    )
    parser.add_argument(
        "--answer-date-column",
        type=int,
        default=3,
        help="answer_date を取得する列番号 (1始まり)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="取り込み前に既存データを削除",
    )
    args = parser.parse_args()

    targets = load_targets(args.targets)

    conn = sqlite3.connect(args.db)
    try:
        init_db(conn)

        if args.truncate:
            conn.execute(f"DELETE FROM {TABLE_NAME}")

        total_inserted = 0
        for curriculum, target in targets.items():
            spreadsheet_key = target.get("spreadsheet_key")
            sheet_name = target.get("sheet_name")
            if not spreadsheet_key or not sheet_name:
                print(f"[SKIP] {curriculum}: spreadsheet_key or sheet_name が不足")
                continue

            try:
                rows = fetch_sheet_rows(spreadsheet_key, sheet_name)
            except Exception as e:  # noqa: BLE001
                print(f"[ERROR] {curriculum}: シート取得失敗 ({e})")
                continue

            inserted = upsert_rows(
                conn,
                curriculum=curriculum,
                rows=rows,
                line_name_column=args.line_name_column,
                answer_date_column=args.answer_date_column,
            )
            total_inserted += inserted
            print(f"[OK] {curriculum}: {inserted} 件取り込み")

        conn.commit()
        print(f"完了: 合計 {total_inserted} 件取り込みました。 DB={args.db}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
