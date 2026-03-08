const canvasRenderer = L.canvas({ padding: 0.5 });

const map = L.map('map', {
  preferCanvas: true
}).setView([28.00156, 34.42779], 15);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

const state = {
  points: [],
  pointIds: new Set(),
  legend: null,
  enabledMeasurements: new Set(),
  layerGroups: new Map(),
  defaultZoom: 15,
  hasFitBounds: false,
  maxTimeMs: 0,
  isRestoringHistory: false,
  historyRestored: false,
};

function ensureHistoryLayer(measurementName) {
  const key = `history:${measurementName}`;
  if (!state.historyLayerGroups.has(key)) {
    state.historyLayerGroups.set(key, L.layerGroup().addTo(map));
  }
  return state.historyLayerGroups.get(key);
}
console.log('Client points count:', state.points.length);
const startBtn = document.getElementById('startBtn');
const statusEl = document.getElementById('status');
const timeSlider = document.getElementById('timeSlider');
const timeLabel = document.getElementById('timeLabel');
const legendEl = document.getElementById('legend');
const togglesEl = document.getElementById('measurementToggles');
const zoomInput = document.getElementById('zoomInput');
const zoomValue = document.getElementById('zoomValue');

function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let value = bytes;
  while (value >= 1024 && i < units.length - 1) { value /= 1024; i += 1; }
  return `${value.toFixed(1)} ${units[i]}`;
}

function getMeasurementColor(measurementName, value) {
  const config = state.legend.legends[measurementName];
  if (!config) return null;

  if (config.type === 'numeric') {
    const num = Number(value);
    if (Number.isNaN(num)) return null;

    const match = config.thresholds.find((item) => num >= item.min && num < item.max);
    return match ? match.color : null;
  }

  const normalizedValue = String(value ?? '').trim().toUpperCase();
  const discrete = config.thresholds.find(
    (item) => String(item.value ?? '').trim().toUpperCase() === normalizedValue
  );

  return discrete ? discrete.color : null;
}

function getMeasurementOffsetIndex(measurementName) {
  const enabled = Array.from(state.legend ? Object.keys(state.legend.legends) : []);
  const index = enabled.indexOf(measurementName);
  return index >= 0 ? index : 0;
}

function offsetPoint(lat, lng, measurementName) {
  const index = getMeasurementOffsetIndex(measurementName);
  return [lat + (index * 0.00009), lng + (index * 0.00006)];
}

function buildPopup(point, measurementName) {
  const value = point.measurements[measurementName];
  return `<strong>${measurementName}</strong><br>Time: ${point.time || '-'}<br>Route: ${point.route_id}<br>Value: ${value ?? '-'}<br>Lat/Lng: ${point.lat.toFixed(6)}, ${point.lng.toFixed(6)}`;
}

function ensureLayer(measurementName) {
  if (!state.layerGroups.has(measurementName)) state.layerGroups.set(measurementName, L.layerGroup().addTo(map));
  return state.layerGroups.get(measurementName);
}

function renderLegend() {
  legendEl.innerHTML = '';
  Object.entries(state.legend.legends).forEach(([name, config]) => {
    if (!state.enabledMeasurements.has(name)) return;
    const wrapper = document.createElement('div');
    const title = document.createElement('div');
    title.className = 'small';
    title.style.marginBottom = '6px';
    title.textContent = config.label;
    wrapper.appendChild(title);
    config.thresholds.forEach((item) => {
      const row = document.createElement('div');
      row.className = 'legend-item';
      row.innerHTML = `<span class="swatch" style="background:${item.color}"></span><span>${item.label || item.value}</span>`;
      wrapper.appendChild(row);
    });
    legendEl.appendChild(wrapper);
  });
}

function renderMeasurementToggles() {
  togglesEl.innerHTML = '';
  Object.entries(state.legend.legends).forEach(([name, config]) => {
    const row = document.createElement('label');
    row.className = 'toggle-item';
    const checked = state.enabledMeasurements.has(name) ? 'checked' : '';
    row.innerHTML = `<input type="checkbox" data-measurement="${name}" ${checked} /> <span>${config.label}</span>`;
    togglesEl.appendChild(row);
  });
  togglesEl.querySelectorAll('input[type="checkbox"]').forEach((input) => {
    input.addEventListener('change', (event) => {
      const measurement = event.target.dataset.measurement;
      if (event.target.checked) state.enabledMeasurements.add(measurement);
      else state.enabledMeasurements.delete(measurement);
      renderLegend();
      scheduleRedraw(true);
    });
  });
}

function getVisiblePoints() {
  if (!state.points.length) return [];

  const sliderValue = Number(timeSlider.value);
  let visible;

  if (sliderValue === 100) {
    visible = state.points;
  } else {
    const threshold = Math.floor((sliderValue / 100) * state.maxTimeMs);

    let low = 0;
    let high = state.points.length - 1;
    let lastValidIndex = -1;

    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      const t = state.points[mid].time_ms || 0;

      if (t <= threshold) {
        lastValidIndex = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }

    visible = lastValidIndex >= 0 ? state.points.slice(0, lastValidIndex + 1) : [];
  }

  if (visible.length <= MAX_RENDER_POINTS) return visible;

  const step = Math.ceil(visible.length / MAX_RENDER_POINTS);
  const sampled = [];
  for (let i = 0; i < visible.length; i += step) {
    sampled.push(visible[i]);
  }
  return sampled;
}

function redraw() {
  state.layerGroups.forEach((group) => group.clearLayers());

  const visiblePoints = getVisiblePoints()
    .slice()
    .sort((a, b) => {
      const routeCompare = String(a.route_id).localeCompare(String(b.route_id));
      if (routeCompare !== 0) return routeCompare;
      return (a.time_ms || 0) - (b.time_ms || 0);
    });

  const bounds = [];
  const enabledMeasurements = Array.from(state.enabledMeasurements);

  for (const measurementName of enabledMeasurements) {
    const layer = ensureLayer(measurementName);

    // route_id -> array of segments
    const routeSegments = new Map();
    const routeColors = new Map();

    for (const point of visiblePoints) {
      const value = point.measurements?.[measurementName];
      if (value === null || value === undefined || value === '') continue;

      const [lat, lng] = offsetPoint(point.lat, point.lng, measurementName);
      const color = getMeasurementColor(measurementName, value);
      if (!color) continue;

      L.circleMarker([lat, lng], {
        renderer: canvasRenderer,
        radius: 4,
        color,
        weight: 1,
        fillColor: color,
        fillOpacity: 0.9
      })
        .bindTooltip(buildTooltip(point), {
          sticky: true,
          direction: 'top',
          opacity: 0.95
        })
        .addTo(layer);

      bounds.push([lat, lng]);

      if (!routeSegments.has(point.route_id)) {
        routeSegments.set(point.route_id, [[[lat, lng, point.time_ms || 0]]]);
      } else {
        const segments = routeSegments.get(point.route_id);
        const currentSegment = segments[segments.length - 1];
        const prev = currentSegment[currentSegment.length - 1];

        let shouldBreak = false;

        if (prev) {
          const prevLat = prev[0];
          const prevLng = prev[1];
          const prevTime = prev[2] || 0;
          const currTime = point.time_ms || 0;

          const dist = distanceMeters(prevLat, prevLng, lat, lng);
          const timeGap = Math.abs(currTime - prevTime);

          // tune these values if needed
          if (dist > 80 || timeGap > 5000) {
            shouldBreak = true;
          }
        }

        if (shouldBreak) {
          segments.push([[lat, lng, point.time_ms || 0]]);
        } else {
          currentSegment.push([lat, lng, point.time_ms || 0]);
        }
      }

      if (!routeColors.has(point.route_id)) {
        routeColors.set(point.route_id, color);
      }
    }

    for (const [routeId, segments] of routeSegments.entries()) {
      for (const segment of segments) {
        if (segment.length <= 1) continue;

        let finalPoints = segment.map((p) => [p[0], p[1]]);

        if (finalPoints.length > 1000) {
          const step = Math.ceil(finalPoints.length / 1000);
          finalPoints = [];
          for (let i = 0; i < segment.length; i += step) {
            finalPoints.push([segment[i][0], segment[i][1]]);
          }
        }

        L.polyline(finalPoints, {
          renderer: canvasRenderer,
          color: routeColors.get(routeId) || '#6b7280',
          weight: 2,
          opacity: 0.7
        }).addTo(layer);
      }
    }
  }

  if (bounds.length && !state.hasFitBounds) {
    map.fitBounds(bounds, { padding: [20, 20], maxZoom: state.defaultZoom });
    state.hasFitBounds = true;
  }

  const visibleCount = visiblePoints.length;
  const latest = visibleCount ? visiblePoints[visibleCount - 1].time : '-';
  timeLabel.textContent =
    Number(timeSlider.value) === 100
      ? `Showing points (${visibleCount})`
      : `Showing ${visibleCount} points up to ${latest}`;
}
function distanceMeters(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;

  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) *
    Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function buildTooltip(point) {
  const lines = [
    `<strong>Time:</strong> ${point.time || '-'}`,
    `<strong>Route:</strong> ${point.route_id || '-'}`,
    `<strong>Lat/Lng:</strong> ${Number(point.lat).toFixed(6)}, ${Number(point.lng).toFixed(6)}`
  ];

  const measurements = point.measurements || {};
  for (const [name, value] of Object.entries(measurements)) {
    if (value === null || value === undefined || value === '') continue;
    lines.push(`<strong>${name}:</strong> ${value}`);
  }

  return lines.join('<br>');
}

let redrawScheduled = false;
let lastRedrawAt = 0;
const REDRAW_INTERVAL_MS = 1000;

function scheduleRedraw(force = false) {
  const now = Date.now();

  if (!force && now - lastRedrawAt < REDRAW_INTERVAL_MS) {
    return;
  }

  if (redrawScheduled) return;
  redrawScheduled = true;

  requestAnimationFrame(() => {
    redrawScheduled = false;
    lastRedrawAt = Date.now();
    redraw();
  });
}

function updateStatus(status) {
  statusEl.innerHTML = `
    <div>Rows processed: <strong>${status.rows_processed ?? 0}</strong></div>
    <div>Bytes streamed: <strong>${formatBytes(status.bytes_read ?? 0)}</strong> / ${formatBytes(status.file_size ?? 0)}</div>
    <div>Chunk size: <strong>${formatBytes(status.chunk_size_bytes ?? 0)}</strong></div>
    <div>Delay: <strong>${status.stream_delay_seconds ?? 0}s</strong></div>
    <div>Completed: <strong>${status.completed ? 'Yes' : 'No'}</strong></div>
    <div>Total history: <strong>${status.history_points_count ?? '-'}</strong></div>
    <div>Rendered snapshot: <strong>${status.snapshot_points_count ?? state.points.length}</strong></div>
  `;
  if (status.started) startBtn.disabled = true;
}
function ingestPoints(newPoints) {
  let added = false;

  for (const point of newPoints) {
    if (!state.pointIds.has(point.id)) {
      state.pointIds.add(point.id);
      state.points.push(point);

      const t = point.time_ms || 0;
      if (t > state.maxTimeMs) state.maxTimeMs = t;

      added = true;
    }
  }
  if (added) {
    scheduleRedraw(true);
  }
}
const MAX_RENDER_POINTS = 20000;
const HISTORY_BATCH_SIZE = 10000;

async function loadFullHistory() {
  state.isRestoringHistory = true;
  state.historyRestored = false;

  state.points = [];
  state.pointIds = new Set();
  state.maxTimeMs = 0;
  state.hasFitBounds = false;

  let offset = 0;
  let hasMore = true;
  let batchCount = 0;
  let lastReturned = 0;
  let total = 0;

  while (hasMore) {
    const response = await fetch(`/api/history?offset=${offset}&limit=${HISTORY_BATCH_SIZE}`);
    const payload = await response.json();

    ingestPoints(payload.points || []);

    lastReturned = payload.returned;
    total = payload.total;
    offset += payload.returned;
    hasMore = payload.has_more;
    batchCount += 1;

    statusEl.innerHTML = `
      <div>History loaded: <strong>${offset}</strong> / <strong>${total}</strong></div>
      <div>Batch size: <strong>${payload.limit}</strong></div>
    `;

    if (batchCount % 5 === 0) {
      scheduleRedraw(true);
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  state.isRestoringHistory = false;
  state.historyRestored = true;
  scheduleRedraw(true);

  console.log("history restore done. last batch:", lastReturned, "total loaded:", offset);
}

async function bootstrap() {
  const response = await fetch('/api/config');
  const payload = await response.json();

  state.legend = payload.legend;
  state.enabledMeasurements = new Set(payload.available_measurements || []);
  updateStatus(payload.state);
  renderMeasurementToggles();
  renderLegend();

  state.points = [];
  state.pointIds = new Set();
  state.maxTimeMs = 0;
  state.hasFitBounds = false;

  ingestPoints(payload.points || []);
  scheduleRedraw(true);

  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);

  socket.addEventListener('message', (event) => {
    const payload = JSON.parse(event.data);

    if (payload.type === 'snapshot') {
      state.legend = payload.legend;
      state.enabledMeasurements = new Set(payload.available_measurements || []);
      renderMeasurementToggles();
      renderLegend();
      updateStatus(payload.state);
    }

    if (payload.type === 'chunk') {
      ingestPoints(payload.points || []);
      updateStatus(payload.state);
    }

    if (payload.type === 'completed') {
      updateStatus(payload.state);
    }
  });
}

startBtn.addEventListener('click', async () => {
  startBtn.disabled = true;
  await fetch('/api/start', { method: 'POST' });
});
timeSlider.addEventListener('input', () => scheduleRedraw(true));
zoomInput.addEventListener('input', (event) => {
  state.defaultZoom = Number(event.target.value);
  zoomValue.textContent = event.target.value;
  map.setZoom(state.defaultZoom);
  state.hasFitBounds = false;
  scheduleRedraw(true);
});
bootstrap();
