from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "measurements.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS measurements (
                id INTEGER PRIMARY KEY,
                time_text TEXT,
                time_ms INTEGER,
                route_id TEXT,
                lat REAL,
                lng REAL,
                measurements_json TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_measurements_time_ms ON measurements(time_ms)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_measurements_route_id ON measurements(route_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_measurements_id ON measurements(id)")
        conn.commit()
    finally:
        conn.close()


def insert_points(points: list[dict[str, Any]]) -> None:
    if not points:
        return

    conn = get_connection()
    try:
        conn.executemany(
            """
            INSERT OR IGNORE INTO measurements (
                id, time_text, time_ms, route_id, lat, lng, measurements_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    point["id"],
                    point.get("time"),
                    point.get("time_ms"),
                    point.get("route_id"),
                    point.get("lat"),
                    point.get("lng"),
                    json.dumps(point.get("measurements", {}), ensure_ascii=False),
                )
                for point in points
            ],
        )
        conn.commit()
    finally:
        conn.close()


def count_points() -> int:
    conn = get_connection()
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM measurements").fetchone()
        return int(row["c"])
    finally:
        conn.close()


def fetch_points(offset: int = 0, limit: int = 10000) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, time_text, time_ms, route_id, lat, lng, measurements_json
            FROM measurements
            ORDER BY id
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()

        return [
            {
                "id": row["id"],
                "time": row["time_text"],
                "time_ms": row["time_ms"],
                "route_id": row["route_id"],
                "lat": row["lat"],
                "lng": row["lng"],
                "measurements": json.loads(row["measurements_json"] or "{}"),
            }
            for row in rows
        ]
    finally:
        conn.close()


def fetch_sampled_snapshot(max_points: int = 5000) -> list[dict[str, Any]]:
    total = count_points()
    if total == 0:
        return []

    if total <= max_points:
        return fetch_points(0, total)

    step = max(1, total // max_points)
    conn = get_connection()
    try:
        rows = conn.execute(
            f"""
            SELECT id, time_text, time_ms, route_id, lat, lng, measurements_json
            FROM measurements
            WHERE (id % ?) = 0
            ORDER BY id
            LIMIT ?
            """,
            (step, max_points),
        ).fetchall()

        points = [
            {
                "id": row["id"],
                "time": row["time_text"],
                "time_ms": row["time_ms"],
                "route_id": row["route_id"],
                "lat": row["lat"],
                "lng": row["lng"],
                "measurements": json.loads(row["measurements_json"] or "{}"),
            }
            for row in rows
        ]
        return points
    finally:
        conn.close()

def clear_points() -> None:
    conn = get_connection()
    try:
        conn.execute("DELETE FROM measurements")
        conn.commit()
    finally:
        conn.close()