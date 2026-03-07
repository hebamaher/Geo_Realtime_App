from __future__ import annotations

import asyncio
import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi import Query

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_FILE = PROJECT_DIR / "data" / "data.FMT"
LEGEND_FILE = BASE_DIR / "config" / "legend.yaml"

CHUNK_SIZE_BYTES = 10 * 1024 * 1024
STREAM_DELAY_SECONDS = 1.0
MAX_POINTS_IN_MEMORY = 200_000

@dataclass
class StreamState:
    started: bool = False
    completed: bool = False
    bytes_read: int = 0
    rows_processed: int = 0

class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        stale: list[WebSocket] = []
        for connection in self.active_connections:
            try:
                await connection.send_json(payload)
            except Exception:
                stale.append(connection)
        for connection in stale:
            self.disconnect(connection)

class RealTimeMeasurementService:
    def __init__(self, data_path: Path, legend_path: Path) -> None:
        self.data_path = data_path
        self.legend_path = legend_path
        self.manager = ConnectionManager()
        self.state = StreamState()
        self.legend_config = self._load_legend_config()
        self.points: list[dict[str, Any]] = []
        self.available_measurements = list(self.legend_config["legends"].keys())
        self._stream_lock = asyncio.Lock()
        self.next_row_id = 0

    def _load_legend_config(self) -> dict[str, Any]:
        with self.legend_path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, "", "NaN"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_time_to_ms(value: str | None) -> int | None:
        if not value:
            return None
        try:
            hh, mm, rest = value.split(":")
            ss, ms = rest.split(".")
            return ((int(hh) * 3600 + int(mm) * 60 + int(ss)) * 1000) + int(ms)
        except ValueError:
            return None

    def _get_row_value(self, row: dict[str, Any], column_name: str) -> Any:
        return row.get(column_name)
    def _build_measurements(self, row: dict[str, Any]) -> dict[str, Any]:
        measurements: dict[str, Any] = {}

        for column_name, config in self.legend_config.get("legends", {}).items():
            raw_value = self._get_row_value(row, column_name)
            measurement_type = config.get("type")

            if measurement_type == "numeric":
                measurements[column_name] = self._safe_float(raw_value)
            else:
                measurements[column_name] = (str(raw_value).strip() if raw_value not in (None, "") else None)

        return measurements

    def _normalize_row(self, row: dict[str, Any], row_id: int) -> dict[str, Any] | None:
        lat = self._safe_float(row.get("All-Latitude"))
        lng = self._safe_float(row.get("All-Longitude"))
        if lat is None or lng is None:
            return None

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
        with self.data_path.open("rb") as handle:
            header_bytes = handle.readline()
            if not header_bytes:
                return

            header_line = header_bytes.decode("utf-8", errors="ignore")
            leftover = b""

            while True:
                chunk = handle.read(CHUNK_SIZE_BYTES)
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

                combined = leftover + chunk
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

    async def start_streaming(self) -> None:
        if self.state.started:
            return

        async with self._stream_lock:
            if self.state.started:
                return

            self.state.started = True
            for rows, bytes_read in self._iter_rows_in_chunks():
                normalized: list[dict[str, Any]] = []
                for row in rows:
                    point = self._normalize_row(row, self.next_row_id)
                    self.next_row_id += 1    
                    if point is not None:
                        normalized.append(point)

                self.state.bytes_read += bytes_read
                self.state.rows_processed += len(normalized)
                self.points.extend(normalized)

                if normalized:
                    await self.manager.broadcast(
                        {
                            "type": "chunk",
                            "points": normalized,
                            "state": self.status_payload(),
                        }
                    )
                await asyncio.sleep(STREAM_DELAY_SECONDS)

            self.state.completed = True
            await self.manager.broadcast({"type": "completed", "state": self.status_payload()})
            print("Backend points count:", len(self.points))

    def status_payload(self) -> dict[str, Any]:
        file_size = self.data_path.stat().st_size if self.data_path.exists() else 0
        return {
            "started": self.state.started,
            "completed": self.state.completed,
            "bytes_read": self.state.bytes_read,
            "rows_processed": self.state.rows_processed,
            "file_size": file_size,
            "chunk_size_bytes": CHUNK_SIZE_BYTES,
            "stream_delay_seconds": STREAM_DELAY_SECONDS,
        }
    
    def current_state_payload(self) -> dict[str, Any]:
        return {
            "legend": self.legend_config,
            "available_measurements": self.available_measurements,
            "points": self.points,
            "state": self.status_payload(),
        }

service = RealTimeMeasurementService(DATA_FILE, LEGEND_FILE)
app = FastAPI(title="Geo-tagged Real-time Measurements")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/config")
async def config() -> JSONResponse:
    return JSONResponse(service.current_state_payload())


@app.get("/api/status")
async def status() -> JSONResponse:
    return JSONResponse(service.status_payload())


@app.post("/api/start")
async def start() -> JSONResponse:
    asyncio.create_task(service.start_streaming())
    return JSONResponse({"message": "Streaming started", "state": service.status_payload()})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await service.manager.connect(websocket)
    try:
        await websocket.send_json({"type": "snapshot", "legend": service.legend_config, "available_measurements": service.available_measurements, "state": service.status_payload(),})
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        service.manager.disconnect(websocket)
    except Exception:
        service.manager.disconnect(websocket)

@app.get("/api/history")
async def history(offset: int = 0, limit: int = 10000) -> JSONResponse:
    total = len(service.points)
    items = service.points[offset: offset + limit]
    return JSONResponse({
        "points": items,
        "offset": offset,
        "limit": limit,
        "returned": len(items),
        "total": total,
        "has_more": offset + len(items) < total,
    })

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
