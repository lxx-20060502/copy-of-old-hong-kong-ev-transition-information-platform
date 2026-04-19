const clientConfig = window.clientPageConfig || {};
const clientContent = document.getElementById('clientContent');
const menuItems = [...document.querySelectorAll('.client-side-item')];
const DEFAULT_LOCAL_BACKEND_ORIGIN = 'http://127.0.0.1:8000';

let activeView = 'intro';
let currentJobId = null;
let currentJob = null;
let pollTimer = null;
let exportUrl = null;
let chargingLeafletMap = null;
let chargingLeafletPoints = [];

function computeBackendOrigin() {
  const override = window.localStorage?.getItem('hkEvBackendOrigin')?.trim();
  if (override) return override.replace(/\/$/, '');

  const queryValue = new URLSearchParams(window.location.search).get('backend');
  if (queryValue) {
    const normalized = queryValue.trim().replace(/\/$/, '');
    try {
      window.localStorage?.setItem('hkEvBackendOrigin', normalized);
    } catch (error) {
      // Ignore storage errors.
    }
    return normalized;
  }

  const host = window.location.hostname;
  if (host === '127.0.0.1' || host === 'localhost') return window.location.origin;
  if (host.endsWith('github.io')) return DEFAULT_LOCAL_BACKEND_ORIGIN;
  return window.location.origin;
}

const BACKEND_ORIGIN = computeBackendOrigin();
const apiUrl = (path) => `${BACKEND_ORIGIN}${path}`;

function injectMapPatchStyles() {
  if (document.getElementById('clientChargingMapPatch')) return;

  const style = document.createElement('style');
  style.id = 'clientChargingMapPatch';
  style.textContent = `
    .charging-map-result {
      position: relative;
      overflow: visible;
    }

    .charging-map-shell {
      margin-top: 1.5rem;
      display: grid;
      gap: 1rem;
    }

    .charging-map-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 0.75rem;
      align-items: center;
      justify-content: space-between;
      color: rgba(232, 236, 244, 0.88);
      font-size: 0.96rem;
    }

    .charging-map-hint {
      color: rgba(194, 204, 220, 0.82);
      font-size: 0.92rem;
    }

    .charging-map-board {
      position: relative;
      min-height: 520px;
      border-radius: 22px;
      overflow: hidden;
      background: rgba(7, 12, 20, 0.30);
      border: 1px solid rgba(165, 186, 221, 0.18);
      backdrop-filter: blur(24px) saturate(145%);
      -webkit-backdrop-filter: blur(24px) saturate(145%);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.04), 0 24px 70px rgba(0,0,0,0.24);
    }

    .charging-map-board::before,
    .charging-map-board::after {
      display: none;
    }

    #chargingLeafletMap {
      position: relative;
      z-index: 1;
      width: 100%;
      height: 520px;
    }

    .leaflet-container {
      width: 100%;
      height: 100%;
      background: rgba(7, 12, 20, 0.36);
      font: inherit;
    }

    .leaflet-control-attribution {
      background: rgba(9, 15, 26, 0.78) !important;
      color: rgba(219, 227, 239, 0.82);
      backdrop-filter: blur(8px);
      -webkit-backdrop-filter: blur(8px);
    }

    .leaflet-control-attribution a {
      color: rgba(141, 198, 255, 0.96);
    }

    .leaflet-popup-content-wrapper,
    .leaflet-popup-tip {
      background: rgba(9, 15, 26, 0.94);
      color: rgba(235, 240, 246, 0.95);
      border: 1px solid rgba(147, 180, 226, 0.24);
      box-shadow: 0 18px 48px rgba(0, 0, 0, 0.32);
      backdrop-filter: blur(10px);
      -webkit-backdrop-filter: blur(10px);
    }

    .leaflet-popup-content {
      margin: 0.9rem 1rem;
      line-height: 1.45;
    }

    .leaflet-popup-close-button {
      display: none;
    }

    .charging-map-empty {
      padding: 2rem;
      border-radius: 18px;
      border: 1px dashed rgba(180, 196, 226, 0.24);
      background: rgba(255,255,255,0.03);
      color: rgba(224, 229, 238, 0.86);
      text-align: center;
    }

    .charging-map-tooltip {
      position: absolute;
      z-index: 5;
      min-width: 240px;
      max-width: min(360px, calc(100% - 24px));
      padding: 0.95rem 1rem;
      border-radius: 16px;
      background: rgba(9, 15, 26, 0.94);
      border: 1px solid rgba(147, 180, 226, 0.28);
      box-shadow: 0 18px 48px rgba(0, 0, 0, 0.32);
      color: rgba(235, 240, 246, 0.95);
      backdrop-filter: blur(12px);
      pointer-events: none;
      opacity: 0;
      transform: translateY(8px);
      transition: opacity 0.12s ease, transform 0.12s ease;
    }

    .charging-map-tooltip.is-visible {
      opacity: 1;
      transform: translateY(0);
    }

    .charging-map-tooltip-title {
      font-size: 1rem;
      font-weight: 600;
      margin-bottom: 0.65rem;
      line-height: 1.35;
    }

    .charging-map-tooltip-grid {
      display: grid;
      gap: 0.45rem;
    }

    .charging-map-tooltip-row {
      display: grid;
      grid-template-columns: 100px 1fr;
      gap: 0.55rem;
      align-items: start;
      font-size: 0.9rem;
      line-height: 1.35;
    }

    .charging-map-tooltip-label {
      color: rgba(176, 190, 214, 0.78);
    }

    .charging-map-tooltip-value {
      color: rgba(236, 241, 246, 0.96);
      word-break: break-word;
    }

    .charging-map-legend {
      display: flex;
      align-items: center;
      gap: 0.55rem;
      color: rgba(188, 201, 221, 0.82);
      font-size: 0.88rem;
    }

    .charging-map-legend-dot {
      width: 11px;
      height: 11px;
      border-radius: 999px;
      background: rgba(114, 178, 255, 0.92);
      box-shadow: 0 0 0 3px rgba(114, 178, 255, 0.18);
      flex: 0 0 auto;
    }

    @media (max-width: 900px) {
      #chargingLeafletMap {
        height: 460px;
      }

      .charging-map-board {
        min-height: 460px;
      }

      .charging-map-tooltip {
        min-width: 220px;
      }
    }

    @media (max-width: 640px) {
      .charging-map-toolbar {
        flex-direction: column;
        align-items: flex-start;
      }

      #chargingLeafletMap {
        height: 390px;
      }

      .charging-map-board {
        min-height: 390px;
      }

      .charging-map-tooltip {
        left: 12px !important;
        right: 12px;
        top: auto !important;
        bottom: 12px;
        max-width: none;
      }

      .charging-map-tooltip-row {
        grid-template-columns: 88px 1fr;
      }
    }
  `;
  document.head.appendChild(style);
}

function injectVehicleChartPatchStyles() {
  if (document.getElementById('clientVehicleChartPatch')) return;

  const style = document.createElement('style');
  style.id = 'clientVehicleChartPatch';
  style.textContent = `
    .vehicle-chart-shell {
      margin-top: 1.2rem;
    }

    .vehicle-chart-board {
      position: relative;
      overflow: hidden;
      border-radius: 24px;
      border: 1px solid rgba(176, 196, 228, 0.18);
      background: rgba(8, 12, 20, 0.18);
      backdrop-filter: blur(26px) saturate(145%);
      -webkit-backdrop-filter: blur(26px) saturate(145%);
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.05), 0 24px 70px rgba(0,0,0,0.26);
    }

    .vehicle-chart-board::before,
    .vehicle-chart-board::after {
      content: '';
      position: absolute;
      inset: 0;
      pointer-events: none;
    }

    .vehicle-chart-board::before {
      background:
        linear-gradient(rgba(255,255,255,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px);
      background-size: 84px 84px;
      opacity: 0.5;
    }

    .vehicle-chart-board::after {
      background: linear-gradient(180deg, rgba(0,0,0,0.14) 0%, rgba(0,0,0,0.26) 100%);
    }

    .vehicle-chart-scroll {
      position: relative;
      z-index: 1;
      overflow-x: auto;
      overflow-y: hidden;
      padding: 20px 20px 12px;
      scrollbar-width: thin;
      scrollbar-color: rgba(210, 222, 245, 0.28) transparent;
    }

    .vehicle-chart-scroll::-webkit-scrollbar {
      height: 10px;
    }

    .vehicle-chart-scroll::-webkit-scrollbar-track {
      background: transparent;
    }

    .vehicle-chart-scroll::-webkit-scrollbar-thumb {
      border-radius: 999px;
      background: rgba(210, 222, 245, 0.24);
    }

    .vehicle-chart-svg {
      display: block;
      height: 520px;
    }

    .vehicle-chart-grid {
      stroke: rgba(190, 204, 230, 0.12);
      stroke-width: 1;
    }

    .vehicle-chart-axis,
    .vehicle-chart-axis-label,
    .vehicle-chart-legend,
    .vehicle-chart-empty {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }

    .vehicle-chart-axis {
      stroke: rgba(210, 222, 245, 0.2);
      stroke-width: 1;
    }

    .vehicle-chart-axis-label {
      fill: rgba(214, 224, 241, 0.78);
      font-size: 12px;
      letter-spacing: 0.02em;
    }

    .vehicle-chart-x-label {
      fill: rgba(190, 203, 227, 0.72);
      font-size: 11px;
    }

    .vehicle-chart-legend {
      position: absolute;
      top: 18px;
      right: 22px;
      z-index: 2;
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 14px;
      color: rgba(228, 235, 244, 0.88);
      font-size: 0.88rem;
    }

    .vehicle-chart-legend-item {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      white-space: nowrap;
    }

    .vehicle-chart-legend-swatch {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      box-shadow: 0 0 0 3px rgba(255,255,255,0.08);
      flex: 0 0 auto;
    }

    .vehicle-chart-bar {
      cursor: pointer;
      transition: opacity 0.15s ease, transform 0.15s ease, filter 0.15s ease;
      transform-box: fill-box;
      transform-origin: center bottom;
    }

    .vehicle-chart-bar:hover,
    .vehicle-chart-bar:focus-visible,
    .vehicle-chart-bar.is-active {
      opacity: 1;
      filter: brightness(1.15);
      transform: scaleY(1.02);
      outline: none;
    }

    .vehicle-chart-bar.is-dimmed {
      opacity: 0.42;
    }

    .vehicle-chart-tooltip {
      position: absolute;
      z-index: 4;
      min-width: 230px;
      max-width: min(320px, calc(100% - 24px));
      padding: 0.95rem 1rem;
      border-radius: 16px;
      background: rgba(9, 15, 26, 0.94);
      border: 1px solid rgba(147, 180, 226, 0.28);
      box-shadow: 0 18px 48px rgba(0, 0, 0, 0.32);
      color: rgba(235, 240, 246, 0.95);
      backdrop-filter: blur(12px);
      -webkit-backdrop-filter: blur(12px);
      pointer-events: none;
      opacity: 0;
      transform: translateY(8px);
      transition: opacity 0.12s ease, transform 0.12s ease;
    }

    .vehicle-chart-tooltip.is-visible {
      opacity: 1;
      transform: translateY(0);
    }

    .vehicle-chart-tooltip-title {
      margin-bottom: 0.6rem;
      font-size: 1rem;
      font-weight: 600;
      line-height: 1.35;
    }

    .vehicle-chart-tooltip-grid {
      display: grid;
      gap: 0.42rem;
    }

    .vehicle-chart-tooltip-row {
      display: grid;
      grid-template-columns: 118px 1fr;
      gap: 0.55rem;
      align-items: start;
      font-size: 0.9rem;
      line-height: 1.35;
    }

    .vehicle-chart-tooltip-label {
      color: rgba(176, 190, 214, 0.78);
    }

    .vehicle-chart-tooltip-value {
      color: rgba(236, 241, 246, 0.96);
      word-break: break-word;
    }

    .vehicle-chart-empty {
      padding: 2rem;
      border-radius: 18px;
      border: 1px dashed rgba(180, 196, 226, 0.24);
      background: rgba(255,255,255,0.03);
      color: rgba(224, 229, 238, 0.86);
      text-align: center;
    }

    @media (max-width: 720px) {
      .vehicle-chart-scroll {
        padding: 64px 14px 10px;
      }

      .vehicle-chart-legend {
        left: 14px;
        right: 14px;
        top: 14px;
        justify-content: flex-start;
      }

      .vehicle-chart-svg {
        height: 440px;
      }

      .vehicle-chart-tooltip {
        left: 12px !important;
        right: 12px;
        top: auto !important;
        bottom: 12px;
        max-width: none;
      }

      .vehicle-chart-tooltip-row {
        grid-template-columns: 96px 1fr;
      }
    }
  `;
  document.head.appendChild(style);
}

function revokeExportUrl() {
  if (exportUrl) {
    URL.revokeObjectURL(exportUrl);
    exportUrl = null;
  }
}

function setActiveMenu(view) {
  activeView = view;
  menuItems.forEach((item) => item.classList.toggle('active', item.dataset.view === view));
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function extractDownloadPayload(job) {
  const logs = Array.isArray(job?.logs) ? job.logs : [];
  for (let index = logs.length - 1; index >= 0; index -= 1) {
    const log = logs[index];
    if (log.step === 'download' && log.status === 'completed') return log.response_body;
  }
  return null;
}

function normalizePayload(payload) {
  if (payload == null) return [{ raw_response: '' }];

  let parsed = payload;
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch (error) {
      return [{ raw_response: parsed }];
    }
  }

  if (parsed && typeof parsed === 'object' && parsed.type === 'FeatureCollection' && Array.isArray(parsed.features)) {
    return parsed.features.map((feature, index) => {
      const properties = feature?.properties && typeof feature.properties === 'object' ? feature.properties : {};
      const geometry = feature?.geometry && typeof feature.geometry === 'object' ? feature.geometry : {};
      const coords = Array.isArray(geometry.coordinates) ? geometry.coordinates : [];
      return {
        row_number: index + 1,
        ...properties,
        geometry_type: geometry.type || '',
        longitude: coords[0] ?? '',
        latitude: coords[1] ?? '',
      };
    });
  }

  if (Array.isArray(parsed)) {
    if (parsed.every((item) => item && typeof item === 'object' && !Array.isArray(item))) return parsed;
    return parsed.map((item) => ({ value: typeof item === 'object' ? JSON.stringify(item) : String(item) }));
  }

  if (parsed && typeof parsed === 'object') {
    const arrayEntry = Object.values(parsed).find(
      (value) => Array.isArray(value) && value.every((item) => item && typeof item === 'object' && !Array.isArray(item)),
    );
    if (arrayEntry) return arrayEntry;
    return [parsed];
  }

  return [{ raw_response: String(parsed) }];
}

function rowsToCsv(rows) {
  const headers = [
    ...rows.reduce((set, row) => {
      Object.keys(row || {}).forEach((key) => set.add(key));
      return set;
    }, new Set()),
  ];

  const escapeCsv = (value) => {
    const text = value == null ? '' : String(value);
    if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
    return text;
  };

  const lines = [headers.map(escapeCsv).join(',')];
  rows.forEach((row) => {
    lines.push(headers.map((header) => escapeCsv(row?.[header])).join(','));
  });
  return `${lines.join('\n')}\n`;
}

function authBadges(job) {
  const checks = job?.authorization_checks || {};
  const labels = [];
  Object.keys(checks).forEach((key) => {
    if (checks[key] && clientConfig.authDisplay?.[key]) labels.push(clientConfig.authDisplay[key]);
  });
  return labels;
}

function buildStatCards(rows) {
  if (!rows.length) return [];

  const cards = [{ label: 'Rows returned', value: rows.length }];
  const keys = Object.keys(rows[0] || {});

  const periodKey = keys.find((key) => /month|period|date/i.test(key));
  if (periodKey) {
    const values = rows.map((row) => row?.[periodKey]).filter(Boolean).map(String).sort();
    if (values.length) cards.push({ label: 'Latest period', value: values[values.length - 1] });
  }

  const regKey = keys.find((key) => /no_reg|registration/i.test(key));
  if (regKey) {
    const total = rows.reduce((sum, row) => sum + (Number(row?.[regKey]) || 0), 0);
    if (total) cards.push({ label: 'Total registrations', value: total.toLocaleString() });
  }

  const licKey = keys.find((key) => /no_lic|licen/i.test(key));
  if (licKey) {
    const total = rows.reduce((sum, row) => sum + (Number(row?.[licKey]) || 0), 0);
    if (total) cards.push({ label: 'Total licensed', value: total.toLocaleString() });
  }

  if (clientConfig.pageKey === 'charging') {
    const operatorKey = keys.find((key) => /operator/i.test(key));
    if (operatorKey) {
      const distinct = new Set(rows.map((row) => row?.[operatorKey]).filter(Boolean)).size;
      if (distinct) cards.push({ label: 'Operators', value: distinct });
    }

    const activeKey = keys.find((key) => /status|available|availability/i.test(key));
    if (activeKey) {
      const nonEmpty = rows.filter((row) => row?.[activeKey] !== '' && row?.[activeKey] != null).length;
      if (nonEmpty) cards.push({ label: 'Live status fields', value: nonEmpty });
    }
  }

  return cards.slice(0, 4);
}

function renderTable(rows, limit = 12) {
  if (!rows.length) {
    return '<div class="client-empty-state">No authorized rows were returned for preview.</div>';
  }

  const headers = Object.keys(rows[0]).slice(0, 8);
  const body = rows
    .slice(0, limit)
    .map((row) => `<tr>${headers.map((header) => `<td>${escapeHtml(row?.[header] ?? '')}</td>`).join('')}</tr>`)
    .join('');

  return `<div class="client-table-wrap"><table class="client-table"><thead><tr>${headers
    .map((header) => `<th>${escapeHtml(header)}</th>`)
    .join('')}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function humanizeKey(key) {
  return String(key)
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function toNumeric(value) {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const normalized = trimmed.replace(/,/g, '');
    const parsed = Number(normalized);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function pickValue(row, keys) {
  for (const key of keys) {
    if (row?.[key] !== undefined && row?.[key] !== null && String(row[key]).trim() !== '') return row[key];
  }
  return '';
}

function extractCoordinates(row) {
  const longitude = toNumeric(
    pickValue(row, [
      'longitude',
      'lon',
      'lng',
      'long',
      'x',
      'Longitude',
      'LONGITUDE',
      'LONG',
      'X',
    ]),
  );
  const latitude = toNumeric(
    pickValue(row, ['latitude', 'lat', 'y', 'Latitude', 'LATITUDE', 'LAT', 'Y']),
  );

  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) return null;
  if (longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90) return null;

  return { longitude, latitude };
}

function summarizeChargingRow(row) {
  const title =
    pickValue(row, [
      'name',
      'station_name',
      'station',
      'charger_name',
      'charger',
      'location_name',
      'cp_name',
      'park_name',
      'address',
    ]) || `Charging Point ${row?.row_number || ''}`.trim();

  const candidateKeys = [
    ['Operator', ['operator', 'operator_name', 'network', 'provider']],
    ['Status', ['status', 'availability', 'available', 'charger_status']],
    ['District', ['district', 'area', 'region']],
    ['Address', ['address', 'location', 'venue', 'site']],
    ['Connector', ['connector', 'connector_type', 'socket', 'socket_type']],
    ['Medium', ['medium_count', 'medium', 'medium_charger', 'medium_chargers']],
    ['Quick', ['quick_count', 'quick', 'quick_charger', 'quick_chargers']],
    ['Standard', ['standard_count', 'standard', 'standard_charger', 'standard_chargers']],
  ];

  const details = [];
  for (const [label, keys] of candidateKeys) {
    const value = pickValue(row, keys);
    if (value !== '') details.push({ label, value: String(value) });
  }

  if (!details.length) {
    Object.entries(row || {})
      .filter(([key]) => !/^(row_number|geometry_type|longitude|latitude|lon|lng|lat|x|y)$/i.test(key))
      .slice(0, 8)
      .forEach(([key, value]) => {
        if (value !== '' && value != null) details.push({ label: humanizeKey(key), value: String(value) });
      });
  }

  return { title, details: details.slice(0, 8) };
}

function buildChargingPoints(rows) {
  return rows
    .map((row, index) => {
      const coords = extractCoordinates(row);
      if (!coords) return null;
      return {
        id: index,
        row,
        ...coords,
        ...summarizeChargingRow(row),
      };
    })
    .filter(Boolean);
}

function getMapBounds(points) {
  const longitudes = points.map((point) => point.longitude);
  const latitudes = points.map((point) => point.latitude);
  const minLon = Math.min(...longitudes);
  const maxLon = Math.max(...longitudes);
  const minLat = Math.min(...latitudes);
  const maxLat = Math.max(...latitudes);
  const lonPad = Math.max((maxLon - minLon) * 0.08, 0.015);
  const latPad = Math.max((maxLat - minLat) * 0.1, 0.012);
  return {
    minLon: minLon - lonPad,
    maxLon: maxLon + lonPad,
    minLat: minLat - latPad,
    maxLat: maxLat + latPad,
  };
}

function makeMapProjector(bounds, width, height, margin) {
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const lonSpan = Math.max(bounds.maxLon - bounds.minLon, 0.0001);
  const latSpan = Math.max(bounds.maxLat - bounds.minLat, 0.0001);

  return (longitude, latitude) => {
    const x = margin.left + ((longitude - bounds.minLon) / lonSpan) * innerWidth;
    const y = margin.top + ((bounds.maxLat - latitude) / latSpan) * innerHeight;
    return { x, y };
  };
}

function formatCoordinate(value) {
  return Number(value).toFixed(5);
}

function renderChargingMap(rows) {
  injectMapPatchStyles();

  const points = buildChargingPoints(rows);
  chargingLeafletPoints = points;

  if (!points.length) {
    return `<div class="charging-map-empty">The authorized charging payload did not contain usable coordinate fields, so the controlled map view could not be drawn.</div>`;
  }

  return `
    <section class="charging-map-shell" aria-label="Controlled charging map result">
      <div class="charging-map-toolbar">
        <div class="charging-map-hint">Move the mouse onto any point to view its charger details. Moving the mouse away hides the info automatically.</div>
        <div class="charging-map-legend"><span class="charging-map-legend-dot"></span><span>Authorized charging point</span></div>
      </div>
      <div class="charging-map-board" id="chargingMapBoard">
        <div
          id="chargingLeafletMap"
          aria-label="Hong Kong charging network map"
          style="position: relative; z-index: 1; width: 100%; height: 520px;"
        ></div>
      </div>
    </section>
  `;
}

function formatPeriodLabel(value) {
  const text = String(value ?? '').trim();
  if (/^\d{6}$/.test(text)) return `${text.slice(0, 4)}-${text.slice(4, 6)}`;
  return text;
}

function getVehicleSeriesMeta(index) {
  const palette = [
    { fill: 'rgba(147, 197, 253, 0.92)', stroke: 'rgba(255,255,255,0.92)' },
    { fill: 'rgba(251, 146, 60, 0.90)', stroke: 'rgba(255,255,255,0.88)' },
    { fill: 'rgba(167, 139, 250, 0.90)', stroke: 'rgba(255,255,255,0.88)' },
    { fill: 'rgba(52, 211, 153, 0.88)', stroke: 'rgba(255,255,255,0.88)' },
  ];
  return palette[index % palette.length];
}

function buildVehicleChartRows(rows) {
  if (!rows.length) return { labelKey: '', seriesKeys: [], entries: [] };

  const sample = rows[0] || {};
  const keys = Object.keys(sample);
  const labelKey = keys.find((key) => /month|period|date/i.test(key)) || keys[0] || '';
  const seriesKeys = keys.filter(
    (key) =>
      key !== labelKey &&
      key !== 'row_number' &&
      rows.some((row) => toNumeric(row?.[key]) != null),
  );

  const entries = rows.map((row, index) => {
    const label = row?.[labelKey] ?? `Row ${index + 1}`;
    const values = Object.fromEntries(
      seriesKeys.map((key) => [key, toNumeric(row?.[key]) ?? 0]),
    );
    return { index, label, row, values };
  });

  return { labelKey, seriesKeys, entries };
}

function renderVehicleBarChart(rows) {
  injectVehicleChartPatchStyles();

  const { seriesKeys, entries } = buildVehicleChartRows(rows);
  if (!entries.length || !seriesKeys.length) {
    return '<div class="vehicle-chart-empty">The authorized vehicle-market payload did not contain numeric series that could be rendered as a bar chart.</div>';
  }

  const width = Math.max(1400, entries.length * Math.max(24, seriesKeys.length * 12 + 10) + 150);
  const height = 520;
  const margin = { top: 54, right: 28, bottom: 64, left: 84 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const maxValue = Math.max(
    1,
    ...entries.flatMap((entry) => seriesKeys.map((key) => entry.values[key] || 0)),
  );
  const stepCount = 5;
  const axisMax = Math.ceil(maxValue / stepCount) * stepCount;
  const groupWidth = innerWidth / Math.max(entries.length, 1);
  const gap = Math.min(6, Math.max(2, groupWidth * 0.18));
  const barWidth = Math.max(4, Math.min(20, (groupWidth - gap) / Math.max(seriesKeys.length, 1)));
  const totalBarsWidth = barWidth * seriesKeys.length;
  const baseXOffset = Math.max(0, (groupWidth - totalBarsWidth) / 2);
  const yForValue = (value) => margin.top + innerHeight - (value / axisMax) * innerHeight;

  const horizontalLines = [];
  for (let i = 0; i <= stepCount; i += 1) {
    const value = (axisMax / stepCount) * i;
    const y = yForValue(value);
    horizontalLines.push(`
      <line class="vehicle-chart-grid" x1="${margin.left}" y1="${y.toFixed(2)}" x2="${width - margin.right}" y2="${y.toFixed(2)}"></line>
      <text class="vehicle-chart-axis-label" x="${margin.left - 12}" y="${(y + 4).toFixed(2)}" text-anchor="end">${Math.round(value).toLocaleString()}</text>
    `);
  }

  const xLabels = entries
    .map((entry, index) => {
      if (entries.length > 24 && index % 12 !== 0 && index !== entries.length - 1) return '';
      const x = margin.left + groupWidth * index + groupWidth / 2;
      return `<text class="vehicle-chart-axis-label vehicle-chart-x-label" x="${x.toFixed(2)}" y="${height - 22}" text-anchor="middle">${escapeHtml(formatPeriodLabel(entry.label))}</text>`;
    })
    .join('');

  const bars = entries
    .map((entry, entryIndex) => {
      const tooltipRows = [
        { label: humanizeKey('month'), value: formatPeriodLabel(entry.label) },
        ...seriesKeys.map((key) => ({ label: humanizeKey(key), value: (entry.values[key] || 0).toLocaleString() })),
      ];
      const detailPayload = escapeHtml(JSON.stringify(tooltipRows));

      return seriesKeys
        .map((key, seriesIndex) => {
          const value = entry.values[key] || 0;
          const x = margin.left + groupWidth * entryIndex + baseXOffset + seriesIndex * barWidth;
          const y = yForValue(value);
          const barHeight = Math.max(1.5, margin.top + innerHeight - y);
          const meta = getVehicleSeriesMeta(seriesIndex);
          return `
            <rect
              class="vehicle-chart-bar"
              x="${x.toFixed(2)}"
              y="${y.toFixed(2)}"
              width="${Math.max(2.8, barWidth - 1.2).toFixed(2)}"
              height="${barHeight.toFixed(2)}"
              rx="3.5"
              fill="${meta.fill}"
              stroke="${meta.stroke}"
              stroke-width="0.65"
              tabindex="0"
              role="button"
              aria-label="${escapeHtml(`${formatPeriodLabel(entry.label)} ${humanizeKey(key)} ${value}`)}"
              data-entry-index="${entry.index}"
              data-series-key="${escapeHtml(key)}"
              data-label="${escapeHtml(formatPeriodLabel(entry.label))}"
              data-details-json="${detailPayload}"
            ></rect>
          `;
        })
        .join('');
    })
    .join('');

  const legend = seriesKeys
    .map((key, index) => {
      const meta = getVehicleSeriesMeta(index);
      return `
        <span class="vehicle-chart-legend-item">
          <span class="vehicle-chart-legend-swatch" style="background:${meta.fill}"></span>
          <span>${escapeHtml(humanizeKey(key))}</span>
        </span>
      `;
    })
    .join('');

  return `
    <section class="vehicle-chart-shell" aria-label="Controlled vehicle market chart result">
      <div class="vehicle-chart-board" id="vehicleChartBoard">
        <div class="vehicle-chart-legend">${legend}</div>
        <div class="vehicle-chart-scroll">
          <svg class="vehicle-chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMinYMid meet" aria-label="Vehicle market bar chart">
            <line class="vehicle-chart-axis" x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}"></line>
            <line class="vehicle-chart-axis" x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}"></line>
            ${horizontalLines.join('')}
            ${bars}
            ${xLabels}
          </svg>
        </div>
        <div class="vehicle-chart-tooltip" id="vehicleChartTooltip" aria-hidden="true"></div>
      </div>
    </section>
  `;
}

function attachVehicleChartInteractions() {
  const board = document.getElementById('vehicleChartBoard');
  const tooltip = document.getElementById('vehicleChartTooltip');
  if (!board || !tooltip) return;

  const bars = [...board.querySelectorAll('.vehicle-chart-bar')];

  const hideTooltip = () => {
    tooltip.classList.remove('is-visible');
    tooltip.setAttribute('aria-hidden', 'true');
    bars.forEach((bar) => {
      bar.classList.remove('is-active');
      bar.classList.remove('is-dimmed');
    });
  };

  const showTooltip = (bar, event) => {
    const entryIndex = bar.dataset.entryIndex;
    bars.forEach((node) => {
      const match = node.dataset.entryIndex === entryIndex;
      node.classList.toggle('is-active', node === bar);
      node.classList.toggle('is-dimmed', !match);
    });

    let parsedDetails = [];
    try {
      parsedDetails = JSON.parse(bar.dataset.detailsJson || '[]');
    } catch (error) {
      parsedDetails = [];
    }

    const details = parsedDetails
      .map(
        (item) => `
          <div class="vehicle-chart-tooltip-row">
            <div class="vehicle-chart-tooltip-label">${escapeHtml(item?.label || '')}</div>
            <div class="vehicle-chart-tooltip-value">${escapeHtml(item?.value || '')}</div>
          </div>
        `,
      )
      .join('');

    tooltip.innerHTML = `
      <div class="vehicle-chart-tooltip-title">${escapeHtml(bar.dataset.label || '')}</div>
      <div class="vehicle-chart-tooltip-grid">${details}</div>
    `;
    tooltip.classList.add('is-visible');
    tooltip.setAttribute('aria-hidden', 'false');
    positionTooltip(board, tooltip, event.clientX, event.clientY);
  };

  bars.forEach((bar) => {
    bar.addEventListener('mouseenter', (event) => showTooltip(bar, event));
    bar.addEventListener('mousemove', (event) => showTooltip(bar, event));
    bar.addEventListener('mouseleave', hideTooltip);
    bar.addEventListener('focus', (event) => showTooltip(bar, event));
    bar.addEventListener('blur', hideTooltip);
  });

  board.addEventListener('mouseleave', hideTooltip);
}

function positionTooltip(board, tooltip, clientX, clientY) {
  const boardRect = board.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  const xOffset = 16;
  const yOffset = 18;
  const maxLeft = boardRect.width - tooltipRect.width - 12;
  const maxTop = boardRect.height - tooltipRect.height - 12;

  let left = clientX - boardRect.left + xOffset;
  let top = clientY - boardRect.top - tooltipRect.height - yOffset;

  if (left > maxLeft) left = clientX - boardRect.left - tooltipRect.width - xOffset;
  if (left < 12) left = 12;
  if (top < 12) top = clientY - boardRect.top + yOffset;
  if (top > maxTop) top = maxTop;
  if (top < 12) top = 12;

  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function attachChargingMapInteractions() {
  const board = document.getElementById('chargingMapBoard');
  const mapRoot = document.getElementById('chargingLeafletMap');
  if (!board || !mapRoot) return;

  if (chargingLeafletMap) {
    chargingLeafletMap.remove();
    chargingLeafletMap = null;
  }

  if (typeof window.L === 'undefined') {
    mapRoot.innerHTML = `<div class="charging-map-empty">Leaflet failed to load, so the interactive map could not be displayed.</div>`;
    return;
  }

  const points = Array.isArray(chargingLeafletPoints) ? chargingLeafletPoints : [];
  if (!points.length) {
    mapRoot.innerHTML = `<div class="charging-map-empty">No valid charging points were available for the interactive map.</div>`;
    return;
  }

  chargingLeafletMap = window.L.map(mapRoot, {
    zoomControl: true,
    scrollWheelZoom: true,
    preferCanvas: true,
  });

  window.L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors',
  }).addTo(chargingLeafletMap);

  const latLngs = [];

  points.forEach((point) => {
    const lat = Number(point.latitude);
    const lon = Number(point.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

    const latLng = [lat, lon];
    latLngs.push(latLng);

    const detailsHtml = (point.details || [])
      .map(
        (item) => `
          <div style="display:grid; grid-template-columns: 92px 1fr; gap: 0.45rem; align-items:start;">
            <div style="font-weight: 600; color: #cdd7e5;">${escapeHtml(item?.label || '')}</div>
            <div>${escapeHtml(item?.value || '')}</div>
          </div>
        `,
      )
      .join('');

    const popupHtml = `
      <div style="min-width: 220px; line-height: 1.45;">
        <div style="font-weight: 700; margin-bottom: 0.6rem; font-size: 0.98rem;">
          ${escapeHtml(point.title || 'Charging Point')}
        </div>
        <div style="display:grid; gap: 0.38rem;">
          ${detailsHtml}
          <div style="display:grid; grid-template-columns: 92px 1fr; gap: 0.45rem; align-items:start;">
            <div style="font-weight: 600; color: #cdd7e5;">Coordinate</div>
            <div>${escapeHtml(formatCoordinate(lat))}, ${escapeHtml(formatCoordinate(lon))}</div>
          </div>
        </div>
      </div>
    `;

    const marker = window.L.circleMarker(latLng, {
      radius: 7,
      weight: 1.5,
      color: 'rgba(255,255,255,0.95)',
      fillColor: 'rgba(114,178,255,0.92)',
      fillOpacity: 0.96,
    }).addTo(chargingLeafletMap);

    marker.bindPopup(popupHtml, {
      closeButton: false,
      offset: [0, -2],
    });

    marker.on('mouseover', () => marker.openPopup());
    marker.on('mouseout', () => marker.closePopup());
    marker.on('click', () => marker.openPopup());
  });

  if (!latLngs.length) return;

  if (latLngs.length === 1) {
    chargingLeafletMap.setView(latLngs[0], 16);
  } else {
    chargingLeafletMap.fitBounds(latLngs, { padding: [30, 30] });
  }

  window.setTimeout(() => {
    if (chargingLeafletMap) chargingLeafletMap.invalidateSize();
  }, 0);
}

function renderIntro() {
  clientContent.innerHTML = `<article class="client-copy-panel client-copy-panel-left"><h1>${clientConfig.introTitle}</h1><div class="client-copy-body">${clientConfig.introHtml}</div></article>`;
}

function renderContracts() {
  clientContent.innerHTML = `<article class="client-copy-panel client-copy-panel-left"><h1>${clientConfig.contractTitleHtml}</h1><div class="client-copy-body">${clientConfig.contractHtml}</div></article>`;
}

function openAuthorizedResult() {
  if (!currentJob) return;

  const rows = normalizePayload(extractDownloadPayload(currentJob));

  let actionHtml = `<button class="vehicle-download-button" id="closeSessionButton" type="button">${escapeHtml(
    clientConfig.closeLabel || 'Close Session',
  )}</button>`;

  if (clientConfig.allowExport) {
    actionHtml = `<button class="vehicle-download-button" id="exportPermittedButton" type="button">${escapeHtml(
      clientConfig.exportLabel || 'Export Permitted Copy',
    )}</button>${actionHtml}`;
  } else {
    actionHtml = `<button class="vehicle-download-button" id="refreshAuthorizedButton" type="button">${escapeHtml(
      clientConfig.refreshLabel || 'Refresh Authorized View',
    )}</button>${actionHtml}`;
  }

  const bodyHtml =
    clientConfig.pageKey === 'charging'
      ? renderChargingMap(rows)
      : clientConfig.pageKey === 'vehicle'
        ? renderVehicleBarChart(rows)
        : renderTable(rows);

  let resultHtml = '';

  if (clientConfig.pageKey === 'charging') {
    resultHtml = `
      <article class="client-copy-panel client-copy-panel-left charging-map-result client-result-view client-result-view-charging">
        <h1>${clientConfig.resultHeading}</h1>
        ${bodyHtml}
        <div class="client-result-actions">${actionHtml}</div>
        <div class="client-result-footnote">Source: HK Environmental Protection Department</div>
      </article>
    `;
  } else {
    resultHtml = `
      <article class="client-copy-panel client-copy-panel-left charging-map-result client-result-view">
        <h1>${clientConfig.resultHeading}</h1>
        ${bodyHtml}
        <div class="client-result-actions">${actionHtml}</div>
        <div class="client-result-footnote">Exported copies remain subject to the displayed contract terms.</div>
      </article>
    `;
  }

  clientContent.innerHTML = resultHtml;

  document.getElementById('closeSessionButton')?.addEventListener('click', closeSession);
  document.getElementById('refreshAuthorizedButton')?.addEventListener('click', () => {
    renderAccess();
    startWorkflow();
  });
  document.getElementById('exportPermittedButton')?.addEventListener('click', () => {
    revokeExportUrl();
    exportUrl = URL.createObjectURL(new Blob([rowsToCsv(rows)], { type: 'text/csv;charset=utf-8' }));
    const link = document.createElement('a');
    link.href = exportUrl;
    link.download = clientConfig.pageKey === 'vehicle' ? 'permitted-vehicle-market-copy.csv' : 'authorized-export.csv';
    document.body.appendChild(link);
    link.click();
    link.remove();
  });

  if (clientConfig.pageKey === 'charging') attachChargingMapInteractions();
  if (clientConfig.pageKey === 'vehicle') attachVehicleChartInteractions();
}

function renderAccess() {
  clientContent.innerHTML = `
    <article class="client-copy-panel client-copy-panel-center access-panel">
      <button class="access-asset-trigger" type="button" id="accessAssetTrigger">Access Asset</button>
      <p class="access-asset-tip" id="accessAssetTip">Click the text above to access details.</p>
      <div class="vehicle-workflow-error" id="vehicleWorkflowError"></div>
      <div class="vehicle-download-shell" id="vehicleDownloadShell"></div>
    </article>
  `;

  document.getElementById('accessAssetTrigger')?.addEventListener('click', () => startWorkflow());
  if (currentJobId && currentJob) updateWorkflow(currentJob);
}

function renderView(view) {
  revokeExportUrl();
  if (chargingLeafletMap) {
    chargingLeafletMap.remove();
    chargingLeafletMap = null;
  }
  setActiveMenu(view);
  if (view === 'intro') return renderIntro();
  if (view === 'contracts') return renderContracts();
  return renderAccess();
}

function updateWorkflow(job) {
  currentJob = job;
  if (activeView !== 'access') return;

  const tip = document.getElementById('accessAssetTip');
  const error = document.getElementById('vehicleWorkflowError');
  const shell = document.getElementById('vehicleDownloadShell');
  const trigger = document.getElementById('accessAssetTrigger');
  if (!tip || !error || !shell || !trigger) return;

  if (job.status === 'completed') {
    tip.textContent = 'Authorized result is ready.';
    error.textContent = '';
    trigger.disabled = false;
    trigger.classList.remove('is-running');
    shell.innerHTML = `<button class="vehicle-download-button" id="openAuthorizedResultButton" type="button">${escapeHtml(
      clientConfig.resultOpenLabel || 'Open Authorized Result',
    )}</button>`;
    document.getElementById('openAuthorizedResultButton')?.addEventListener('click', openAuthorizedResult);
    return;
  }

  shell.innerHTML = '';

  if (job.status === 'failed') {
    tip.textContent = 'The request stopped before completion.';
    error.textContent = job.error || 'The workflow failed.';
    trigger.disabled = false;
    trigger.classList.remove('is-running');
    return;
  }

  tip.textContent = job.current_message || 'The workflow is running in the background.';
  error.textContent = '';
  trigger.disabled = true;
  trigger.classList.add('is-running');
}

function stopPolling() {
  if (pollTimer) {
    window.clearTimeout(pollTimer);
    pollTimer = null;
  }
}

async function pollJob() {
  if (!currentJobId) return;

  try {
    const response = await fetch(apiUrl(`${clientConfig.jobPrefix}${currentJobId}`));
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    updateWorkflow(payload);
    if (payload.status === 'running') {
      pollTimer = window.setTimeout(pollJob, 1400);
    } else {
      stopPolling();
    }
  } catch (error) {
    const tip = document.getElementById('accessAssetTip');
    const box = document.getElementById('vehicleWorkflowError');
    if (tip) tip.textContent = 'The workflow could not continue.';
    if (box) box.textContent = error.message;
    stopPolling();
  }
}

async function startWorkflow() {
  if (currentJob && currentJob.status === 'running') return;

  renderAccess();
  const tip = document.getElementById('accessAssetTip');
  const errorBox = document.getElementById('vehicleWorkflowError');
  if (tip) tip.textContent = 'Checking local backend status...';
  if (errorBox) errorBox.textContent = '';

  try {
    const health = await fetch(apiUrl('/api/health'));
    if (!health.ok) throw new Error(`HTTP ${health.status}`);
  } catch (error) {
    if (tip) {
      tip.textContent = 'The backend is offline. Start the local Python server before accessing the asset.';
    }
    if (errorBox) errorBox.textContent = error.message;
    return;
  }

  try {
    const response = await fetch(apiUrl(clientConfig.authorizeStartPath), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(clientConfig.startConfig || {}),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    currentJobId = payload.job_id;
    currentJob = { status: 'running', current_message: 'Preparing authorized request...' };
    updateWorkflow(currentJob);
    pollJob();
  } catch (error) {
    if (tip) tip.textContent = 'The request could not be started.';
    if (errorBox) errorBox.textContent = error.message;
  }
}

function closeSession() {
  stopPolling();
  if (chargingLeafletMap) {
    chargingLeafletMap.remove();
    chargingLeafletMap = null;
  }
  currentJobId = null;
  currentJob = null;
  revokeExportUrl();
  renderView('intro');
}

menuItems.forEach((item) => item.addEventListener('click', () => renderView(item.dataset.view)));
window.addEventListener('beforeunload', () => {
  stopPolling();
  revokeExportUrl();
});

renderView('intro');
