# Geo_Realtime_App

A FastAPI + Leaflet web application for streaming, storing, and visualizing geo-tagged measurement data from a large `.FMT` file in near real time.

This project reads measurement records in chunks, normalizes and deduplicates them, stores them in a local database, and displays them on an interactive map with configurable legends, measurement toggles, time-range filtering, and live updates over WebSocket.

---

## Features

- Stream a large `.FMT` file in **10 MB chunks**
- Parse and normalize geo-tagged measurement rows
- Deduplicate repeated records before storing and broadcasting
- Persist processed points in the database
- Broadcast live updates to the frontend using **WebSocket**
- Visualize points on a **Leaflet** map with canvas rendering
- Toggle measurements on/off from the UI
- Filter displayed points using a **start/end time range**
- Edit legend thresholds and colors from the GUI
- Add / remove legend threshold rows dynamically
- Reset the legend configuration to YAML defaults
- Auto-follow the latest streamed point on the map
- Restore historical data from the backend in pages

---

## Tech Stack

### Backend
- Python
- FastAPI
- WebSocket
- YAML
- SQLite (through `app.db` helpers)

### Frontend
- HTML / CSS / JavaScript
- Leaflet

---

## Project Structure

```text
project/
├── app/
│   ├── main.py              # FastAPI app, streaming logic, APIs, websocket
│   ├── db.py                # Database helper functions
│   ├── config/
│   │   └── legend.yaml      # Default legend configuration
│   └── static/
│       ├── index.html       # Frontend layout
│       ├── styles.css       # Frontend styles
│       └── app.js           # Frontend logic and map rendering
├── data/
│   └── data.FMT             # Input measurement file
└── README.md
```
## How It Works
1. File Streaming
The backend reads the .FMT file in 10 MB chunks instead of loading the whole file into memory at once.

2. Row Normalization

3. Deduplication
- Points are deduplicated using a composite key based on:
    - time
    - route ID
    - rounded latitude
    - rounded longitude
    - measurement values

4. Persistence
Deduplicated points are inserted into the database so they can be restored later.

5. Live Broadcast
New points are pushed to all connected clients using WebSocket.

6. Frontend Rendering
- The frontend:
  - loads legend configuration
  - loads an initial sampled snapshot
  - listens for live chunks
  - draws enabled measurements on the map
  - applies time-range filters and legend colors

### Backend API
- GET /
  - Serves the main frontend page.

- GET /api/config
  - Returns the frontend bootstrap payload:
    - active legend config
    - available measurements
    - sampled snapshot points
    - current stream state
    - history count

- GET /api/status:
  - Returns current streaming status.

- POST /api/start:
  - Starts the background file streaming task.

- GET /api/history?offset=0&limit=10000
  - Returns paginated history from the database.
  - Response fields:
    - points
    - offset
    - limit
    - returned
    - total
    - has_more

- GET /api/legend:
  - Returns the active legend configuration.

- PUT /api/legend
  - Updates the active legend configuration after validation.

- POST /api/legend/reset
  - Resets the legend configuration back to the YAML defaults.

- WebSocket /ws
  - Used for live streaming updates.
  - Message types:
    - snapshot
    - chunk
    - completed

### Frontend Features
**Interactive Map**
- Built with Leaflet
- Uses canvas rendering for better performance with many markers
- Supports auto-fit and configurable zoom

**Measurement Toggle Panel**
Users can enable or disable which measurements are shown.

**Time Range Filter**
Two sliders allow filtering points:
  - From
  - To

**Legend Editor**
Users can:
- edit numeric/discrete thresholds
- change colors and labels
- add threshold rows
- remove threshold rows
- reset the legend to defaults
  
**Auto Follow**
The map can follow the latest streamed point as new data arrives.

**Legend Configuration**
The default legend is loaded from:
```text
app/config/legend.yaml
```
At runtime, the active legend is loaded from the database if available; otherwise the YAML config is used and saved as the initial editable configuration.
Supported legend types:
- Numeric
- Discrete

## Database Role
The database is used to:
- store deduplicated points
- support restoring history after refresh
- provide paginated access to old points
- store the editable legend configuration

## Running the Project
1. Create and activate a virtual environment
- Windows PowerShell
```text
python -m venv .venv
.venv\Scripts\Activate.ps1
```
- macOS / Linux
```text
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies
```text
pip install -r requirements.txt
```

3. Make sure the input data file exists
Expected path:
```text
data/data.FMT
```

4. Run the server
```text   
python -m uvicorn app.main:app --app-dir . --reload
```

5. Open the app
Open:
```text
http://127.0.0.1:8000
```

## Typical Workflow
1. Open the app
2. Click Start streaming
3. The backend begins reading the .FMT file in chunks
4. New points are:
    - normalized
    - deduplicated
    - stored in the DB
    - broadcast to the frontend
5. The map updates live
6. Users can:
    - change zoom
    - toggle measurements
    - adjust the time range
    - edit legend thresholds

## Validation Rules for Legend Editing
The legend editor validates input before saving.

**Numeric measurements**
- must contain at least one threshold row
- each row must include:
  - min
  - max
  - color

**Discrete measurements**
- must contain at least one threshold row
- each row must include:
  - value
  - color
