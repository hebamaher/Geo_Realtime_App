// Use Leaflet canvas renderer for better performance with many markers.
const canvasRenderer = L.canvas({ padding: 0.5 });

// Create the map and set the initial center + zoom.
const map = L.map('map', {
  preferCanvas: true
}).setView([28.00156, 34.42779], 15);

// Add OpenStreetMap tiles as the basemap.
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// Central frontend state for points, UI config, and rendering flags.
const state = {
  points: [],                      // All points currently loaded in the browser
  pointIds: new Set(),             // Used to avoid duplicate points
  legend: null,                    // Legend config returned from backend
  enabledMeasurements: new Set(),  // Measurements currently enabled in the UI
  layerGroups: new Map(),          // One Leaflet layer group per measurement
  defaultZoom: 15,                 // User-controlled preferred zoom
  hasFitBounds: false,             // Prevent repeated fitBounds calls
  maxTimeMs: 0,                    // Largest time_ms seen so far
  isRestoringHistory: false,       // True while loading paged history
  historyRestored: false,          // True after history restore completes
  autoFollow: true                 // If true, map follows the latest streamed point
};

console.log('Client points count:', state.points.length);

// Cache references to frequently-used DOM elements.
const startBtn = document.getElementById('startBtn');
const statusEl = document.getElementById('status');
const timeStartSlider = document.getElementById('timeStartSlider');
const timeEndSlider = document.getElementById('timeEndSlider');
const timeLabel = document.getElementById('timeLabel');
const legendEl = document.getElementById('legend');
const togglesEl = document.getElementById('measurementToggles');
const zoomInput = document.getElementById('zoomInput');
const zoomValue = document.getElementById('zoomValue');

// Legend editor modal/state elements.
let editableLegend = null;
const legendSettingsBtn = document.getElementById('legendSettingsBtn');
const legendEditorModal = document.getElementById('legendEditorModal');
const legendEditorContent = document.getElementById('legendEditorContent');
const saveLegendBtn = document.getElementById('saveLegendBtn');
const resetLegendBtn = document.getElementById('resetLegendBtn');
const closeLegendBtn = document.getElementById('closeLegendBtn');

// If you want dragging the map to disable live follow, uncomment this.
// map.on('movestart', () => {
//   state.autoFollow = false;
// });

// Open the legend editor modal and fetch the latest editable legend config.
legendSettingsBtn.addEventListener('click', async () => {
  const response = await fetch('/api/legend');
  editableLegend = await response.json();
  renderLegendEditor();
  legendEditorModal.style.display = 'flex';
});

// Close the legend editor modal.
closeLegendBtn.addEventListener('click', () => {
  legendEditorModal.style.display = 'none';
});

// Render the editable legend UI inside the modal.
function renderLegendEditor() {
  legendEditorContent.innerHTML = '';

  Object.entries(editableLegend.legends).forEach(([name, config]) => {
    const block = document.createElement('div');
    block.className = 'legend-editor-block';

    let html = `<h4>${name}</h4>`;
    html += `<div class="type-label">Type: ${config.type}</div>`;

    config.thresholds.forEach((t, index) => {
      if (config.type === 'numeric') {
        html += `
          <div class="threshold-row threshold-row-with-actions">
            <input data-name="${name}" data-index="${index}" data-field="min" value="${t.min}" placeholder="Min">
            <input data-name="${name}" data-index="${index}" data-field="max" value="${t.max}" placeholder="Max">
            <input data-name="${name}" data-index="${index}" data-field="color" value="${t.color}" placeholder="Color">
            <input data-name="${name}" data-index="${index}" data-field="label" value="${t.label || ''}" placeholder="Label">
            <button
              type="button"
              class="remove-threshold-btn"
              data-name="${name}"
              data-index="${index}"
            >
              Remove
            </button>
          </div>
        `;
      } else {
        html += `
          <div class="threshold-row threshold-row-with-actions">
            <input data-name="${name}" data-index="${index}" data-field="value" value="${t.value}" placeholder="Value">
            <input data-name="${name}" data-index="${index}" data-field="color" value="${t.color}" placeholder="Color">
            <input data-name="${name}" data-index="${index}" data-field="label" value="${t.label || ''}" placeholder="Label">
            <button
              type="button"
              class="remove-threshold-btn"
              data-name="${name}"
              data-index="${index}"
            >
              Remove
            </button>
          </div>
        `;
      }
    });

    html += `
      <div class="threshold-actions">
        <button type="button" class="add-threshold-btn" data-name="${name}">
          Add Row
        </button>
      </div>
    `;

    block.innerHTML = html;
    legendEditorContent.appendChild(block);
  });

  // Re-bind events after rebuilding the editor DOM.
  legendEditorContent.querySelectorAll('input').forEach((input) => {
    input.addEventListener('input', handleLegendEditorInput);
  });

  legendEditorContent.querySelectorAll('.add-threshold-btn').forEach((btn) => {
    btn.addEventListener('click', handleAddThresholdRow);
  });

  legendEditorContent.querySelectorAll('.remove-threshold-btn').forEach((btn) => {
    btn.addEventListener('click', handleRemoveThresholdRow);
  });
}

// Validate and save the edited legend config to the backend.
saveLegendBtn.addEventListener('click', async () => {
  const errors = validateLegendConfig(editableLegend);

  if (errors.length > 0) {
    alert(errors.join("\n"));
    return;
  }

  const response = await fetch('/api/legend', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(editableLegend)
  });

  const payload = await response.json();

  // Update live state with the saved legend.
  state.legend = payload.legend;
  state.enabledMeasurements = new Set(
    payload.available_measurements || Object.keys(payload.legend.legends)
  );

  renderMeasurementToggles();
  renderLegend();
  scheduleRedraw(true);

  legendEditorModal.style.display = 'none';
});

// Reset legend config to backend defaults.
resetLegendBtn.addEventListener('click', async () => {
  const response = await fetch('/api/legend/reset', { method: 'POST' });
  const payload = await response.json();

  state.legend = payload.legend;
  editableLegend = payload.legend;
  state.enabledMeasurements = new Set(
    payload.available_measurements || Object.keys(payload.legend.legends)
  );

  renderLegendEditor();
  renderMeasurementToggles();
  renderLegend();
  scheduleRedraw(true);
});

// Update the editable in-memory legend object when an input changes.
function handleLegendEditorInput(event) {
  const input = event.target;
  const name = input.dataset.name;
  const index = Number(input.dataset.index);
  const field = input.dataset.field;

  let value = input.value;
  const config = editableLegend.legends[name];

  // For numeric thresholds, convert min/max fields to numbers when possible.
  if (config.type === 'numeric' && (field === 'min' || field === 'max')) {
    if (value === 'infinity') value = 'infinity';
    else if (value === '-infinity') value = '-infinity';
    else value = Number(value);
  }

  config.thresholds[index][field] = value;
}

// Add a new threshold row for the selected measurement.
function handleAddThresholdRow(event) {
  const name = event.target.dataset.name;
  const config = editableLegend.legends[name];

  if (config.type === 'numeric') {
    config.thresholds.push({
      min: 0,
      max: 0,
      color: '#9ca3af',
      label: ''
    });
  } else {
    config.thresholds.push({
      value: '',
      color: '#9ca3af',
      label: ''
    });
  }

  renderLegendEditor();
}

// Remove a threshold row, but never allow removing the last remaining row.
function handleRemoveThresholdRow(event) {
  const name = event.target.dataset.name;
  const index = Number(event.target.dataset.index);
  const config = editableLegend.legends[name];

  if (!config.thresholds || config.thresholds.length <= 1) {
    alert('Each measurement must keep at least one threshold row.');
    return;
  }

  config.thresholds.splice(index, 1);
  renderLegendEditor();
}

// Basic frontend validation before sending legend changes to backend.
function validateLegendConfig(legend) {
  const errors = [];

  for (const [name, config] of Object.entries(legend.legends)) {
    if (!config.thresholds || config.thresholds.length === 0) {
      errors.push(`${name} must have at least one threshold row.`);
      continue;
    }

    if (config.type === 'numeric') {
      config.thresholds.forEach((row, i) => {
        const min = row.min;
        const max = row.max;

        const validMin = min === 'infinity' || min === '-infinity' || !Number.isNaN(Number(min));
        const validMax = max === 'infinity' || max === '-infinity' || !Number.isNaN(Number(max));

        if (!validMin) errors.push(`${name} row ${i + 1}: invalid min value`);
        if (!validMax) errors.push(`${name} row ${i + 1}: invalid max value`);

        if (!row.color) errors.push(`${name} row ${i + 1}: color is required`);
      });
    } else {
      const values = new Set();

      config.thresholds.forEach((row, i) => {
        if (!row.value) {
          errors.push(`${name} row ${i + 1}: value is required`);
        }

        if (values.has(row.value)) {
          errors.push(`${name} row ${i + 1}: duplicate value "${row.value}"`);
        }

        values.add(row.value);

        if (!row.color) errors.push(`${name} row ${i + 1}: color is required`);
      });
    }
  }

  return errors;
}

// Convert the two time sliders into an actual [start, end] millisecond window.
function getCurrentTimeWindow() {
  const startPercent = Number(timeStartSlider.value);
  const endPercent = Number(timeEndSlider.value);

  const startThreshold = Math.floor((startPercent / 100) * state.maxTimeMs);
  const endThreshold = Math.floor((endPercent / 100) * state.maxTimeMs);

  return {
    start: Math.min(startThreshold, endThreshold),
    end: Math.max(startThreshold, endThreshold)
  };
}

// Convert raw byte counts into a human-readable string.
function formatBytes(bytes) {
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let i = 0;
  let value = bytes;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i += 1;
  }
  return `${value.toFixed(1)} ${units[i]}`;
}

// Convert string-based infinity values from the legend editor into real JS values.
function normalizeThresholdValue(v) {
  if (v === 'infinity') return Infinity;
  if (v === '-infinity') return -Infinity;
  return Number(v);
}

// Follow the newest point on the map while live streaming is active.
// This will not run during history restore.
function followLatestPoint(point) {
  if (!state.autoFollow) return;
  if (state.isRestoringHistory) return;

  const lat = point.lat;
  const lng = point.lng;

  if (lat == null || lng == null) return;

  // Use panTo so the map moves smoothly to the newest point.
  map.panTo([lat, lng], {
    animate: true,
    duration: 0.5
  });
}

// Decide what color a point should have for a given measurement.
function getMeasurementColor(measurementName, value) {
  // Get the config for this measurement.
  const config = state.legend.legends[measurementName];

  // If the measurement is not in the legend, draw it in gray.
  if (!config) return '#9ca3af';

  if (config.type === 'numeric') {
    // Null/empty numeric values should not be colored.
    if (value === null || value === undefined || value === '') return null;

    const num = Number(value);
    if (Number.isNaN(num)) return null;

    const match = config.thresholds.find((item) => {
      const min = normalizeThresholdValue(item.min);
      const max = normalizeThresholdValue(item.max);
      return num >= min && num < max;
    });

    return match ? match.color : null;
  }

  // find the threshold range it belongs to, return that threshold’s color
  // const match = config.thresholds.find((item) => num >= item.min && num < item.max);
  // If the value does not match any range, return null
  // return match ? match.color : null;

  if (value === null || value === undefined || value === '') return null;

  // Normalize text for case-insensitive discrete matching.
  const normalizedValue = String(value ?? '').trim().toUpperCase();
  const discrete = config.thresholds.find(
    (item) => String(item.value ?? '').trim().toUpperCase() === normalizedValue
  );
  return discrete ? discrete.color : null;
}

// Give each measurement a small numeric index.
function getMeasurementOffsetIndex(measurementName) {
  const enabled = Array.from(state.legend ? Object.keys(state.legend.legends) : []);
  const index = enabled.indexOf(measurementName);
  return index >= 0 ? index : 0;
}

// Add a small offset to latitude/longitude so multiple enabled measurements
// at the same location do not sit exactly on top of each other.
function offsetPoint(lat, lng, measurementName) {
  const index = getMeasurementOffsetIndex(measurementName);
  return [lat + (index * 0.00009), lng + (index * 0.00006)];
}

// Create or retrieve the Leaflet LayerGroup for a measurement.
function ensureLayer(measurementName) {
  if (!state.layerGroups.has(measurementName)) {
    state.layerGroups.set(measurementName, L.layerGroup().addTo(map));
  }
  return state.layerGroups.get(measurementName);
}

// Render the legend panel for currently enabled measurements only.
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

// Render checkbox toggles for all measurements in the legend.
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
    // When a user checks/unchecks a measurement, update state.enabledMeasurements, , call scheduleRedraw(true)
    input.addEventListener('change', (event) => {
      const measurement = event.target.dataset.measurement;
      if (event.target.checked) state.enabledMeasurements.add(measurement);
      else state.enabledMeasurements.delete(measurement);

      // Re-render the legend because visible measurements changed.
      renderLegend();

      // The map and legend update to match the toggles.
      scheduleRedraw(true);
    });
  });
}

// Return points that fall inside the currently selected time window.
// If too many points remain, sample them for performance.
function getVisiblePoints() {
  if (!state.points.length) return [];

  const { start, end } = getCurrentTimeWindow();

  const visible = state.points.filter((point) => {
    const t = point.time_ms || 0;
    return t >= start && t <= end;
  });

  if (visible.length <= MAX_RENDER_POINTS) return visible;

  const step = Math.ceil(visible.length / MAX_RENDER_POINTS);
  const sampled = [];
  for (let i = 0; i < visible.length; i += step) {
    sampled.push(visible[i]);
  }
  return sampled;
}

// Redraw all currently visible markers from scratch.
function redraw() {
  // 1. clear all current layers
  state.layerGroups.forEach((group) => group.clearLayers());

  const visiblePoints = getVisiblePoints();
  const bounds = [];
  const enabledMeasurements = Array.from(state.enabledMeasurements);

  // optional: only enable tooltips when point count is not huge
  const enableTooltips = visiblePoints.length <= 3000;

  // Draw each measurement separately.
  for (const measurementName of enabledMeasurements) {
    const layer = ensureLayer(measurementName);

    for (const point of visiblePoints) {
      const value = point.measurements?.[measurementName];
      if (value === null || value === undefined || value === '') continue;

      // if (point.lat === null || point.lat === undefined || point.lng === null || point.lng === undefined) continue;
      // if (point.time_ms === null || point.time_ms === undefined) continue;

      const [lat, lng] = offsetPoint(point.lat, point.lng, measurementName);
      const color = getMeasurementColor(measurementName, value);
      if (!color) continue;

      const marker = L.circleMarker([lat, lng], {
        renderer: canvasRenderer,
        radius: 4,
        color,
        weight: 1,
        fillColor: color,
        fillOpacity: 0.9
      });

      // Only attach tooltips when the visible count is not too large.
      if (enableTooltips) {
        marker.bindTooltip(buildTooltip(point), {
          sticky: true,
          direction: 'top',
          opacity: 0.95
        });
      }

      marker.addTo(layer);

      // Store drawn positions so map can fit to them once.
      bounds.push([lat, lng]);
    }
  }

  // Fit map to all drawn points once, unless reset later.
  if (bounds.length && !state.hasFitBounds) {
    map.fitBounds(bounds, { padding: [20, 20], maxZoom: state.defaultZoom });
    state.hasFitBounds = true;
  }

  // Update the time range label.
  const visibleCount = visiblePoints.length;
  const first = visibleCount ? visiblePoints[0].time : '-';
  const latest = visibleCount ? visiblePoints[visibleCount - 1].time : '-';

  timeLabel.textContent = visibleCount
    ? `Showing points from ${first} to ${latest}`
    : 'No points in selected range';
}

// Build tooltip HTML for a point.
function buildTooltip(point) {
  const lines = [
    `<strong>Time:</strong> ${point.time || '-'}`,
    `<strong>Route:</strong> ${point.route_id || '-'}`,
    `<strong>Lat/Lng:</strong> ${Number(point.lat).toFixed(6)}, ${Number(point.lng).toFixed(6)}`
  ];

  // const measurements = point.measurements || {};
  // for (const [name, value] of Object.entries(measurements)) {
  //   if (value === null || value === undefined || value === '') continue;
  //   lines.push(`<strong>${name}:</strong> ${value}`);
  // }

  return lines.join('<br>');
}

// Redraw throttling state.
let redrawScheduled = false;
let lastRedrawAt = 0;
const REDRAW_INTERVAL_MS = 1000;

// Schedule a redraw and throttle how often redraw runs.
function scheduleRedraw(force = false) {
  const now = Date.now();

  // If redraws are happening too frequently, skip this one.
  if (!force && now - lastRedrawAt < REDRAW_INTERVAL_MS) {
    return;
  }

  if (redrawScheduled) return;
  redrawScheduled = true;

  // Let the browser choose a suitable frame for the redraw for performance optimization.
  requestAnimationFrame(() => {
    redrawScheduled = false;
    lastRedrawAt = Date.now();
    redraw();
  });
}

// Update the status panel in the sidebar.
function updateStatus(status) {
  statusEl.innerHTML = `
    <div>Bytes streamed: <strong>${formatBytes(status.bytes_read ?? 0)}</strong> / ${formatBytes(status.file_size ?? 0)}</div>
    <div>Chunk size: <strong>${formatBytes(status.chunk_size_bytes ?? 0)}</strong></div>
    <div>Delay: <strong>${status.stream_delay_seconds ?? 0}s</strong></div>
    <div>Completed: <strong>${status.completed ? 'Yes' : 'No'}</strong></div>
  `;

  // Disable start streaming button if streaming is already started.
  if (status.started) startBtn.disabled = true;
}

// Ingest new points from backend into browser memory.
// Follow the latest added point once per batch, not once per row.
function ingestPoints(newPoints) {
  let added = false;
  let latestAddedPoint = null;

  for (const point of newPoints) {
    // if pointId is not seen add to state.points
    if (!state.pointIds.has(point.id)) {
      state.pointIds.add(point.id);
      state.points.push(point);

      const t = point.time_ms || 0;
      if (t > state.maxTimeMs) state.maxTimeMs = t;

      latestAddedPoint = point;
      added = true;
    }
  }

  // Follow only the newest point that was actually added in this batch.
  if (latestAddedPoint) {
    followLatestPoint(latestAddedPoint);
  }

  if (added) {
    scheduleRedraw(true);
  }
}

// Rendering/history limits.
const MAX_RENDER_POINTS = 20000;
const HISTORY_BATCH_SIZE = 10000;

// Restore full history progressively from the backend.
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

// Initialize the app when the page loads.
async function bootstrap() {
  // 1. fetch configs
  const response = await fetch('/api/config');
  const payload = await response.json();

  // 2. sets frontend config
  state.legend = payload.legend;
  state.enabledMeasurements = new Set(payload.available_measurements || []);
  updateStatus(payload.state);
  renderMeasurementToggles();
  renderLegend();

  // 3. clears old frontend state before inserting the fetched snapshot.
  state.points = [];
  state.pointIds = new Set();
  state.maxTimeMs = 0;
  state.hasFitBounds = false;

  // await loadFullHistory();

  // // 4. adds the initial points and forces a redraw.
  ingestPoints(payload.points || []);
  scheduleRedraw(true);

  // 5. opens the live stream channel.
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);

  // 6. handle WebSocket messages
  socket.addEventListener('message', (event) => {
    const payload = JSON.parse(event.data);

    // Updates legend and status.
    if (payload.type === 'snapshot') {
      state.legend = payload.legend;
      state.enabledMeasurements = new Set(payload.available_measurements || []);
      renderMeasurementToggles();
      renderLegend();
      updateStatus(payload.state);
    }

    // Adds incoming streamed points and updates status
    if (payload.type === 'chunk') {
      ingestPoints(payload.points || []);
      updateStatus(payload.state);
    }

    // Marks stream complete in the UI.
    if (payload.type === 'completed') {
      updateStatus(payload.state);
    }
  });
}

// Start backend streaming when user clicks the button.
startBtn.addEventListener('click', async () => {
  startBtn.disabled = true;
  await fetch('/api/start', { method: 'POST' });
});

// Keep the two time sliders ordered and redraw when they change.
timeStartSlider.addEventListener('input', () => {
  if (Number(timeStartSlider.value) > Number(timeEndSlider.value)) {
    timeEndSlider.value = timeStartSlider.value;
  }
  scheduleRedraw(true);
});

timeEndSlider.addEventListener('input', () => {
  if (Number(timeEndSlider.value) < Number(timeStartSlider.value)) {
    timeStartSlider.value = timeEndSlider.value;
  }
  scheduleRedraw(true);
});

// Update map zoom settings and redraw when zoom control changes.
zoomInput.addEventListener('input', (event) => {
  state.defaultZoom = Number(event.target.value);
  zoomValue.textContent = event.target.value;
  map.setZoom(state.defaultZoom);
  state.hasFitBounds = false;
  scheduleRedraw(true);
});

// Start the application.
bootstrap();