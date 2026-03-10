from __future__ import annotations

import asyncio
import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import json
from collections import defaultdict
import yaml
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from app.db import (
    init_db,
    insert_points,
    count_points,
    fetch_points,
    fetch_sampled_snapshot,
    clear_points,
    save_legend_config,
    load_legend_config_from_db,
)

# Resolve important filesystem paths relative to this file.
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_FILE = PROJECT_DIR / "data" / "data.FMT"
LEGEND_FILE = BASE_DIR / "config" / "legend.yaml"

# Stream/file-processing configuration.
CHUNK_SIZE_BYTES = 10 * 1024 * 1024
STREAM_DELAY_SECONDS = 0
# MAX_POINTS_IN_MEMORY = 200_000


@dataclass
class StreamState:
    """
    Hold the current streaming progress/state.

    Attributes:
        started: True once file streaming has begun.
        completed: True once the full file has been processed.
        bytes_read: Number of bytes consumed from the source file.
        rows_processed: Number of valid/deduplicated rows processed so far.
    """
    started: bool = False
    completed: bool = False
    bytes_read: int = 0
    rows_processed: int = 0


class ConnectionManager:
    """
    Manage all active WebSocket clients.

    This class is responsible for:
    - accepting new websocket connections
    - removing disconnected ones
    - broadcasting payloads to every connected client
    """

    def __init__(self) -> None:
        """Initialize the manager with an empty list of active websocket connections."""
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        """Accept a websocket connection and track it as active."""
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a websocket from the active connections list if it exists."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    # Sends  payload to connected the client.
    async def broadcast(self, payload: dict[str, Any]) -> None:
        """
        Broadcast a JSON payload to all connected websocket clients.

        Any websocket that fails during send is considered stale and removed.
        """
        stale: list[WebSocket] = []
        for connection in self.active_connections:
            try:
                await connection.send_json(payload)
            except Exception:
                stale.append(connection)

        for connection in stale:
            self.disconnect(connection)


class RealTimeMeasurementService:
    """
    Main service responsible for:
    - loading legend configuration
    - parsing and normalizing rows from the .FMT file
    - deduplicating rows
    - inserting points into the database
    - broadcasting live chunks to websocket clients
    """

    def __init__(self, data_path: Path, legend_path: Path) -> None:
        """
        Initialize the real-time measurement service.

        Args:
            data_path: Path to the .FMT source data file.
            legend_path: Path to the YAML legend configuration file.
        """
        self.data_path = data_path
        self.legend_path = legend_path
        self.manager = ConnectionManager()
        self.state = StreamState()

        # self.legend_config = self._load_legend_config()
        # Load legend from DB if available, otherwise seed from YAML.
        self.legend_config = self._load_active_legend_config()

        # Cache available measurement names for easier iteration later.
        self.available_measurements = list(self.legend_config["legends"].keys())

        # prevents multiple simultaneous streaming runs
        self._stream_lock = asyncio.Lock()

        # assigns unique IDs to rows as they are processed
        self.next_row_id = 0

        # stores dedupe keys
        self.seen_point_keys: set[tuple[Any, ...]] = set()

    # Reads the YAML legend file and returns it as a Python dictionary.
    def _load_legend_config(self) -> dict[str, Any]:
        """
        Load the legend configuration from the YAML file.

        Returns:
            Parsed legend configuration as a dictionary.
        """
        with self.legend_path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    def _load_active_legend_config(self) -> dict[str, Any]:
        """
        Load the active legend configuration.

        Preference order:
        1. Use the DB-stored legend if one exists.
        2. Otherwise load from YAML, save it to DB, and use that.

        Returns:
            Active legend configuration dictionary.
        """
        db_config = load_legend_config_from_db()
        if db_config:
            return db_config

        yaml_config = self._load_legend_config()
        save_legend_config(yaml_config)
        return yaml_config

    # parsing numeric file fields to convert them to float
    @staticmethod
    def _safe_float(value: Any) -> float | None:
        """
        Safely convert a value to float.

        Returns None for missing/invalid values instead of raising an error.
        """
        if value in (None, "", "NaN"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    # gives a numeric time field used by the frontend time slider.
    @staticmethod
    def _parse_time_to_ms(value: str | None) -> int | None:
        """
        Convert a time string in format HH:MM:SS.mmm into milliseconds.

        Args:
            value: A time string like '02:45:13.641'.

        Returns:
            Milliseconds from midnight, or None if parsing fails.
        """
        if not value:
            return None
        try:
            hh, mm, rest = value.split(":")
            ss, ms = rest.split(".")
            return ((int(hh) * 3600 + int(mm) * 60 + int(ss)) * 1000) + int(ms)
        except ValueError:
            return None

    # returns a raw value from the parsed row dictionary.
    def _get_row_value(self, row: dict[str, Any], column_name: str) -> Any:
        """
        Return the raw value for a specific column from a parsed row.
        """
        return row.get(column_name)

    # builds the measurements dictionary for one point.
    def _build_measurements(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Build the measurement dictionary for a normalized point.

        For every legend-defined measurement:
        - numeric fields are parsed with _safe_float
        - discrete fields are normalized as stripped strings

        Args:
            row: Raw parsed row dictionary.

        Returns:
            A measurement-name -> measurement-value dictionary.
        """
        measurements: dict[str, Any] = {}

        for column_name, config in self.legend_config.get("legends", {}).items():
            # get raw value from row
            raw_value = self._get_row_value(row, column_name)
            measurement_type = config.get("type")

            # if measurement type is numeric -> convert with _safe_float
            if measurement_type == "numeric":
                measurements[column_name] = self._safe_float(raw_value)
            else:
                measurements[column_name] = (
                    str(raw_value).strip() if raw_value not in (None, "") else None
                )

        return measurements

    # Converts one raw CSV row into the frontend/backend point structure.
    def _normalize_row(self, row: dict[str, Any], row_id: int) -> dict[str, Any] | None:
        """
        Convert one raw parsed row into the normalized point structure.

        A row is discarded if latitude or longitude is invalid.

        Args:
            row: Raw parsed CSV row.
            row_id: Unique ID assigned to this row during processing.

        Returns:
            A normalized point dictionary, or None if the row is invalid.
        """
        lat = self._safe_float(row.get("All-Latitude"))
        lng = self._safe_float(row.get("All-Longitude"))

        # drops the row if either is invalid
        if lat is None or lng is None:
            return None

        # creates a standardized point dictionary
        return {
            "id": row_id,
            "time": row.get("Time"),
            "time_ms": self._parse_time_to_ms(row.get("Time")),
            "route_id": row.get("EQ") or "unknown",
            "lat": lat,
            "lng": lng,
            "measurements": self._build_measurements(row),
        }

    def _iter_rows_in_chunks(self):
        """
        Yield parsed rows from the source file in byte-sized chunks.

        The function:
        - reads the file as bytes
        - keeps the header once
        - carries incomplete line fragments in `leftover`
        - parses each completed chunk into DictReader rows

        Yields:
            A tuple of (list_of_rows, bytes_consumed_for_that_chunk).
        """
        with self.data_path.open("rb") as handle:
            # Read and preserve the header so each parsed chunk can be interpreted correctly.
            header_bytes = handle.readline()
            if not header_bytes:
                return

            header_line = header_bytes.decode("utf-8", errors="ignore")
            leftover = b""

            while True:
                # Read the next raw byte chunk from the file.
                chunk = handle.read(CHUNK_SIZE_BYTES)

                # End-of-file handling: flush any remaining buffered partial line(s).
                if not chunk:
                    if leftover:
                        text = leftover.decode("utf-8", errors="ignore")
                        lines = [line for line in text.splitlines() if line.strip()]
                        if lines:
                            reader = csv.DictReader(
                                io.StringIO(header_line + "".join(f"{line}\n" for line in lines)),
                                delimiter="\t",
                            )
                            yield list(reader), len(leftover)
                    break

                # Merge previous partial line with the newly read chunk.
                combined = leftover + chunk

                # Find the last full newline so we only parse complete lines.
                last_newline = combined.rfind(b"\n")
                if last_newline == -1:
                    leftover = combined
                    continue

                complete = combined[: last_newline + 1]
                leftover = combined[last_newline + 1 :]

                text = complete.decode("utf-8", errors="ignore")
                lines = [line for line in text.splitlines() if line.strip()]
                if not lines:
                    continue

                reader = csv.DictReader(
                    io.StringIO(header_line + "".join(f"{line}\n" for line in lines)),
                    delimiter="\t",
                )
                yield list(reader), len(complete)

    # main runtime streaming loop.
    async def start_streaming(self) -> None:
        """
        Stream the source file chunk by chunk.

        For each chunk:
        - normalize rows
        - deduplicate points
        - update stream state
        - insert into DB
        - broadcast to websocket clients

        The stream runs under a lock so only one active streaming task can exist.
        """
        # if self.state.started:
        #     return

        async with self._stream_lock:
            if self.state.started:
                return

            self.state.started = True

            for rows, bytes_read in self._iter_rows_in_chunks():
                normalized: list[dict[str, Any]] = []

                for row in rows:
                    # Every raw row gets a unique ID.
                    point = self._normalize_row(row, self.next_row_id)
                    self.next_row_id += 1
                    if point is not None:
                        normalized.append(point)

                # removes repeated points according to the dedupe key.
                normalized = self._deduplicate_points(normalized)

                # Update progress metrics.
                self.state.bytes_read += bytes_read
                self.state.rows_processed += len(normalized)

                if normalized:
                    # Insert into DB
                    insert_points(normalized)

                    # sends live points to all connected clients.
                    await self.manager.broadcast(
                        {
                            "type": "chunk",
                            "points": normalized,
                            "state": {
                                **self.status_payload(),
                                "history_points_count": count_points(),
                            },
                        }
                    )

                await asyncio.sleep(STREAM_DELAY_SECONDS)

            # After all file chunks are processed, mark completed
            self.state.completed = True
            await self.manager.broadcast(
                {
                    "type": "completed",
                    "state": {
                        **self.status_payload(),
                        "history_points_count": count_points(),
                    },
                }
            )

    # Returns the current stream state as JSON-friendly dict.
    def status_payload(self) -> dict[str, Any]:
        """
        Build a JSON-friendly summary of current stream state.

        Returns:
            A dictionary containing file size and current stream progress values.
        """
        file_size = self.data_path.stat().st_size if self.data_path.exists() else 0
        return {
            "started": self.state.started,
            "completed": self.state.completed,
            "bytes_read": self.state.bytes_read,
            # "rows_processed": self.state.rows_processed,
            "file_size": file_size,
            "chunk_size_bytes": CHUNK_SIZE_BYTES,
            # "stream_delay_seconds": STREAM_DELAY_SECONDS,
        }

    def current_state_payload(self) -> dict[str, Any]:
        """
        Build the full bootstrap payload for the frontend.

        Includes:
        - legend config
        - available measurements
        - sampled snapshot points
        - current stream state

        Returns:
            Full initial state payload for the frontend.
        """
        snapshot = fetch_sampled_snapshot(SNAPSHOT_MAX_POINTS)
        return {
            "legend": self.legend_config,
            "available_measurements": self.available_measurements,
            "points": snapshot,
            "state": {
                **self.status_payload(),
                "history_points_count": count_points(),
                "snapshot_points_count": len(snapshot),
            },
        }

    def reset_state(self) -> None:
        """
        Reset runtime state, row ID counter, and dedupe memory.

        Useful if you later want to restart processing cleanly within the same process.
        """
        self.state = StreamState()
        self.next_row_id = 0
        self.seen_point_keys = set()

    # Builds the unique signature for a point
    def _point_dedupe_key(self, point: dict[str, Any]) -> tuple[Any, ...]:
        """
        Build a deduplication signature for a normalized point.

        The key is based on:
        - time
        - route_id
        - rounded lat/lng
        - all configured measurement values

        Args:
            point: Normalized point dictionary.

        Returns:
            A tuple that uniquely identifies the point for deduplication purposes.
        """
        measurements = tuple(
            (name, point["measurements"].get(name))
            for name in self.available_measurements
        )
        return (
            point.get("time"),
            point.get("route_id"),
            round(point.get("lat", 0.0), 8),
            round(point.get("lng", 0.0), 8),
            measurements,
        )

    # Loops over the points and keeps only those not already seen.
    # reduces repeated rows dramatically before they are stored and streamed.
    def _deduplicate_points(self, points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Deduplicate a list of normalized points using the in-memory seen-point key set.

        Args:
            points: List of normalized points for the current processed chunk.

        Returns:
            List of deduplicated points only.
        """
        deduped: list[dict[str, Any]] = []
        for point in points:
            key = self._point_dedupe_key(point)
            if key in self.seen_point_keys:
                continue
            self.seen_point_keys.add(key)
            deduped.append(point)
        return deduped


def validate_legend_config(config: dict):
    """
    Validate a legend configuration payload before saving it.

    Validation rules:
    - top-level "legends" key must exist
    - each legend entry must define a valid type
    - each legend must contain at least one threshold row
    - numeric thresholds must contain min/max/color
    - discrete thresholds must contain unique value entries

    Args:
        config: Legend configuration dictionary to validate.

    Raises:
        ValueError: If any required structure or field is missing/invalid.
    """
    if "legends" not in config:
        raise ValueError("Missing legends section")

    for name, legend in config["legends"].items():
        if "type" not in legend:
            raise ValueError(f"{name}: missing type")

        if legend["type"] not in ["numeric", "discrete"]:
            raise ValueError(f"{name}: invalid type")

        thresholds = legend.get("thresholds", [])
        if not thresholds:
            raise ValueError(f"{name}: must have at least one threshold")

        if legend["type"] == "numeric":
            for i, row in enumerate(thresholds):
                if "min" not in row or "max" not in row:
                    raise ValueError(f"{name} row {i+1}: min/max missing")
                if "color" not in row:
                    raise ValueError(f"{name} row {i+1}: color missing")
        else:
            seen = set()
            for i, row in enumerate(thresholds):
                val = row.get("value")
                if val is None:
                    raise ValueError(f"{name} row {i+1}: value missing")
                if val in seen:
                    raise ValueError(f"{name}: duplicate value {val}")
                seen.add(val)


# Initialize DB on app startup and clear any old point history.
init_db()
clear_points()

# Create the main measurement streaming service.
service = RealTimeMeasurementService(DATA_FILE, LEGEND_FILE)

# Create FastAPI application instance.
app = FastAPI(title="Geo-tagged Real-time Measurements")

# Allow all CORS requests for development/testing simplicity.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Expose frontend static files under /static.
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/")
async def index() -> FileResponse:
    """
    Serve the main frontend HTML page.
    """
    return FileResponse(BASE_DIR / "static" / "index.html")


SNAPSHOT_MAX_POINTS = 40000


@app.get("/api/config")
# Returns: legend, available_measurements, sampled_snapshot, current_status, history_count
# This is the frontend’s initial bootstrap API.
async def config() -> JSONResponse:
    """
    Return the frontend bootstrap payload.

    Includes:
    - active legend config
    - available measurements
    - sampled point snapshot
    - stream state
    - history count
    """
    snapshot = fetch_sampled_snapshot(SNAPSHOT_MAX_POINTS)
    return JSONResponse({
        "legend": service.legend_config,
        "available_measurements": service.available_measurements,
        "points": snapshot,
        "state": {
            **service.status_payload(),
            "history_points_count": count_points(),
            "snapshot_points_count": len(snapshot),
        },
    })


# Returns stream status
@app.get("/api/status")
async def status() -> JSONResponse:
    """
    Return the current stream status only.
    """
    return JSONResponse(service.status_payload())


# Starts the background streaming task, returns current state
@app.post("/api/start")
async def start() -> JSONResponse:
    """
    Start the background streaming task.

    Returns:
        Confirmation message and current stream state.
    """
    asyncio.create_task(service.start_streaming())
    return JSONResponse({"message": "Streaming started", "state": service.status_payload()})


# Accepts websocket connections.
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    Handle websocket connections for live point streaming.

    On connect:
    - accept websocket
    - send a snapshot payload with legend + current state

    Then keep the connection open while live chunk broadcasts happen separately.
    """
    await service.manager.connect(websocket)
    try:
        await websocket.send_json({
            "type": "snapshot",
            "legend": service.legend_config,
            "available_measurements": service.available_measurements,
            "state": service.status_payload(),
        })
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        service.manager.disconnect(websocket)
    except Exception:
        service.manager.disconnect(websocket)


# Returns paged full history from the database in chunks
@app.get("/api/history")
async def history(
    offset: int = Query(0, ge=0),
    limit: int = Query(10000, ge=1, le=50000),
) -> JSONResponse:
    """
    Return paginated point history from the database.

    Args:
        offset: Number of rows to skip before returning results.
        limit: Maximum number of rows to return.

    Returns:
        A JSON payload with:
        - points: paged point items
        - offset/limit: request paging values
        - returned: actual number of returned rows
        - total: total number of DB rows
        - has_more: whether more pages remain
    """
    total = count_points()
    items = fetch_points(offset=offset, limit=limit)

    return JSONResponse({
        "points": items,
        "offset": offset,
        "limit": limit,
        "returned": len(items),
        "total": total,
        "has_more": offset + len(items) < total,
    })


@app.get("/api/legend")
async def get_legend() -> JSONResponse:
    """
    Return the currently active legend configuration.
    """
    return JSONResponse(service.legend_config)


from fastapi import Body


@app.put("/api/legend")
async def update_legend(payload: dict):
    """
    Validate and save a new legend configuration.

    Args:
        payload: New legend configuration submitted by the frontend.

    Returns:
        The saved legend and updated available measurements.

    On validation failure:
        Returns a JSON error response with HTTP 400.
    """
    try:
        validate_legend_config(payload)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    save_legend_config(payload)

    # Update the live in-memory service state immediately after save.
    service.legend_config = payload
    service.available_measurements = list(payload["legends"].keys())

    return {
        "legend": payload,
        "available_measurements": service.available_measurements,
    }


@app.post("/api/legend/reset")
async def reset_legend() -> JSONResponse:
    """
    Reset the active legend configuration back to the YAML defaults.

    Returns:
        Confirmation payload with the reset legend and measurements.
    """
    yaml_config = service._load_legend_config()
    save_legend_config(yaml_config)
    service.legend_config = yaml_config
    service.available_measurements = list(yaml_config["legends"].keys())

    return JSONResponse({
        "message": "Legend reset to defaults",
        "legend": service.legend_config,
        "available_measurements": service.available_measurements,
    })


if __name__ == "__main__":
    import uvicorn

    # Run the FastAPI app in development mode with auto-reload.
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)