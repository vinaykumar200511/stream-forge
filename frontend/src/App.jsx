import { useEffect, useState } from "react";
import { Background, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import "./App.css";

const initialMetrics = {
  activeTrucks: 1248,
  eventsPerSecond: 13400,
  temperatureAlerts: 12,
  fleetUptime: 99.2,
};

const defaultRoutes = {
  vehicles: [
    {
      id: "truck-204",
      name: "Truck 204",
      speed: 58,
      status: "moving",
      lat: 40.7128,
      lng: -74.006,
      route: [
        { lat: 40.7000, lng: -74.01 },
        { lat: 40.7060, lng: -74.02 },
        { lat: 40.7128, lng: -74.006 },
        { lat: 40.72, lng: -73.99 },
      ],
    },
    {
      id: "truck-118",
      name: "Truck 118",
      speed: 45,
      status: "delayed",
      lat: 40.7484,
      lng: -73.9857,
      route: [
        { lat: 40.76, lng: -73.98 },
        { lat: 40.756, lng: -73.988 },
        { lat: 40.7484, lng: -73.9857 },
        { lat: 40.742, lng: -73.977 },
      ],
    },
    {
      id: "truck-1",
      name: "Truck 1",
      speed: 51,
      status: "moving",
      lat: 40.718,
      lng: -73.997,
      route: [
        { lat: 40.704, lng: -74.016 },
        { lat: 40.71, lng: -74.006 },
        { lat: 40.718, lng: -73.997 },
        { lat: 40.726, lng: -73.985 },
      ],
    },
    {
      id: "truck-2",
      name: "Truck 2",
      speed: 42,
      status: "delayed",
      lat: 40.754,
      lng: -73.976,
      route: [
        { lat: 40.766, lng: -73.991 },
        { lat: 40.76, lng: -73.984 },
        { lat: 40.754, lng: -73.976 },
        { lat: 40.746, lng: -73.968 },
      ],
    },
    {
      id: "truck-3",
      name: "Truck 3",
      speed: 67,
      status: "moving",
      lat: 40.735,
      lng: -73.958,
      route: [
        { lat: 40.744, lng: -73.974 },
        { lat: 40.74, lng: -73.966 },
        { lat: 40.735, lng: -73.958 },
        { lat: 40.728, lng: -73.948 },
      ],
    },
    {
      id: "truck-4",
      name: "Truck 4",
      speed: 38,
      status: "delayed",
      lat: 40.742,
      lng: -73.942,
      route: [
        { lat: 40.75, lng: -73.954 },
        { lat: 40.746, lng: -73.948 },
        { lat: 40.742, lng: -73.942 },
        { lat: 40.736, lng: -73.934 },
      ],
    },
    {
      id: "truck-5",
      name: "Truck 5",
      speed: 55,
      status: "moving",
      lat: 40.724,
      lng: -73.97,
      route: [
        { lat: 40.712, lng: -73.982 },
        { lat: 40.718, lng: -73.976 },
        { lat: 40.724, lng: -73.97 },
        { lat: 40.732, lng: -73.962 },
      ],
    },
    {
      id: "truck-6",
      name: "Truck 6",
      speed: 48,
      status: "moving",
      lat: 40.759,
      lng: -73.952,
      route: [
        { lat: 40.748, lng: -73.966 },
        { lat: 40.754, lng: -73.96 },
        { lat: 40.759, lng: -73.952 },
        { lat: 40.765, lng: -73.944 },
      ],
    },
  ],
};

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

const temperatureTrend = [
  { label: "00:00", value: 3.4 },
  { label: "04:00", value: 3.9 },
  { label: "08:00", value: 4.5 },
  { label: "12:00", value: 5.1 },
  { label: "16:00", value: 4.8 },
  { label: "20:00", value: 5.4 },
  { label: "24:00", value: 4.9 },
];

const tripTrend = [
  { label: "Mon", value: 182 },
  { label: "Tue", value: 208 },
  { label: "Wed", value: 196 },
  { label: "Thu", value: 221 },
  { label: "Fri", value: 238 },
  { label: "Sat", value: 255 },
  { label: "Sun", value: 247 },
];

const formatNumber = (value) =>
  new Intl.NumberFormat("en-US", {
    maximumFractionDigits: value >= 1000 ? 0 : 1,
  }).format(value);

const buildChartPoints = (series, width, height, padding) => {
  const values = series.map((item) => item.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const range = maxValue - minValue || 1;

  return series.map((item, index) => {
    const x = padding + (index / (series.length - 1)) * (width - padding * 2);
    const y = height - padding - ((item.value - minValue) / range) * (height - padding * 2);

    return { ...item, x, y };
  });
};

const getRouteSummary = (truck) => {
  const routeLength = truck.route?.length || 1;
  const completion = Math.min(98, Math.max(52, 60 + routeLength * 8 + (truck.status === "delayed" ? -10 : 8)));
  const thermalVariance = 1.8 + (truck.speed / 25) + (truck.status === "delayed" ? 1.7 : 0.8);
  const etaMinutes = Math.max(8, 32 - truck.speed / 2 + (truck.status === "delayed" ? 12 : 0));

  return {
    completion: Math.round(completion),
    thermalVariance: Number(thermalVariance.toFixed(1)),
    etaMinutes: Math.round(etaMinutes),
    alerts: truck.status === "delayed" ? 2 : 0,
  };
};

const buildFallbackTopology = () => ({
  nodes: [
    {
      id: "producer",
      position: { x: 60, y: 180 },
      data: { label: "Truck Telemetry Producer", status: "healthy" },
      style: {
        background: "#2563eb",
        color: "#ffffff",
        border: "none",
        borderRadius: "8px",
        padding: "12px",
        width: "180px",
      },
    },
    {
      id: "kafka",
      position: { x: 330, y: 180 },
      data: { label: "Kafka: raw-telemetry", status: "live" },
      style: {
        background: "#f97316",
        color: "#ffffff",
        border: "none",
        borderRadius: "8px",
        padding: "12px",
        width: "180px",
      },
    },
    {
      id: "worker1",
      position: { x: 620, y: 90 },
      data: { label: "Worker 01\nPartitions 0-2", status: "healthy" },
      style: {
        background: "#16a34a",
        color: "#ffffff",
        border: "none",
        borderRadius: "8px",
        padding: "12px",
        width: "150px",
      },
    },
    {
      id: "worker2",
      position: { x: 620, y: 270 },
      data: { label: "Worker 02\nPartitions 3-5", status: "healthy" },
      style: {
        background: "#2dd4bf",
        color: "#ffffff",
        border: "none",
        borderRadius: "8px",
        padding: "12px",
        width: "150px",
      },
    },
    {
      id: "aggregator",
      position: { x: 900, y: 180 },
      data: { label: "processed-averages", status: "live" },
      style: {
        background: "#7c3aed",
        color: "#ffffff",
        border: "none",
        borderRadius: "8px",
        padding: "12px",
        width: "180px",
      },
    },
    {
      id: "dashboard",
      position: { x: 1180, y: 180 },
      data: { label: "Fleet Dashboard", status: "streaming" },
      style: {
        background: "#0ea5e9",
        color: "#ffffff",
        border: "none",
        borderRadius: "8px",
        padding: "12px",
        width: "170px",
      },
    },
  ],
  edges: [
    { id: "producer-kafka", source: "producer", target: "kafka", animated: true, label: "13k ev/s" },
    { id: "kafka-worker1", source: "kafka", target: "worker1", animated: true, label: "0-2" },
    { id: "kafka-worker2", source: "kafka", target: "worker2", animated: true, label: "3-5" },
    { id: "worker1-aggregator", source: "worker1", target: "aggregator", animated: true, label: "5m avg" },
    { id: "worker2-aggregator", source: "worker2", target: "aggregator", animated: true, label: "5m avg" },
    { id: "aggregator-dashboard", source: "aggregator", target: "dashboard", animated: true, label: "WebSocket" },
  ],
});

function App() {
  const [metrics, setMetrics] = useState(initialMetrics);
  const [topology, setTopology] = useState(buildFallbackTopology);
  const [routes, setRoutes] = useState(defaultRoutes);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [truckSearch, setTruckSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortOption, setSortOption] = useState("name");

  useEffect(() => {
    const interval = window.setInterval(() => {
      setMetrics((current) => ({
        activeTrucks: Math.max(1180, current.activeTrucks + Math.floor(Math.random() * 21) - 10),
        eventsPerSecond: Math.max(9800, current.eventsPerSecond + Math.floor(Math.random() * 2400) - 1200),
        temperatureAlerts: Math.max(4, current.temperatureAlerts + Math.floor(Math.random() * 5) - 2),
        fleetUptime: Math.min(99.9, Math.max(98.4, Number((current.fleetUptime + (Math.random() * 0.3 - 0.15)).toFixed(1)))),
      }));
    }, 2000);

    return () => window.clearInterval(interval);
  }, []);

  useEffect(() => {
    let isMounted = true;

    const fetchTopology = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/topology`);
        if (!response.ok) {
          throw new Error(`Topology fetch failed: ${response.status}`);
        }

        const data = await response.json();
        if (!isMounted || !data?.nodes || !data?.edges) {
          return;
        }

        setTopology({
          nodes: data.nodes,
          edges: data.edges.map((edge) => ({
            ...edge,
            style: { stroke: "#38bdf8", strokeWidth: 2 },
          })),
        });
        setLastUpdated(new Date(data.updated_at * 1000));
      } catch (error) {
        if (!isMounted) {
          return;
        }
        setTopology(buildFallbackTopology());
        setLastUpdated(null);
      }
    };

    const fetchRoutes = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/routes`);
        if (!response.ok) {
          throw new Error(`Route fetch failed: ${response.status}`);
        }

        const data = await response.json();
        if (isMounted && Array.isArray(data?.vehicles)) {
          setRoutes({ vehicles: data.vehicles });
        }
      } catch (error) {
        if (isMounted) {
          setRoutes(defaultRoutes);
        }
      }
    };

    fetchTopology();
    fetchRoutes();
    const topologyInterval = window.setInterval(fetchTopology, 4000);
    const routeInterval = window.setInterval(fetchRoutes, 5000);

    return () => {
      isMounted = false;
      window.clearInterval(topologyInterval);
      window.clearInterval(routeInterval);
    };
  }, []);

  const statusLabel = metrics.eventsPerSecond > 10000 ? "System: Kafka Live" : "System: Stable";
  const liveSummary = lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : "Fallback topology";
  const truckSummaries = routes.vehicles.map((truck) => ({
    ...truck,
    ...getRouteSummary(truck),
    statusText: truck.status === "delayed" ? "Delayed" : "On schedule",
    statusClass: truck.status === "delayed" ? "warning" : "healthy",
  }));
  const normalizedSearch = truckSearch.trim().toLowerCase();
  const visibleTruckSummaries = truckSummaries
    .filter((truck) => {
      const matchesSearch = !normalizedSearch || `${truck.name} ${truck.id}`.toLowerCase().includes(normalizedSearch);
      const matchesStatus = statusFilter === "all" || truck.status === statusFilter;
      return matchesSearch && matchesStatus;
    })
    .sort((firstTruck, secondTruck) => {
      if (sortOption === "speed") {
        return secondTruck.speed - firstTruck.speed;
      }
      if (sortOption === "eta") {
        return firstTruck.etaMinutes - secondTruck.etaMinutes;
      }
      if (sortOption === "thermal") {
        return secondTruck.thermalVariance - firstTruck.thermalVariance;
      }
      return firstTruck.name.localeCompare(secondTruck.name);
    });
  const hasActiveTruckFilters = Boolean(normalizedSearch) || statusFilter !== "all" || sortOption !== "name";
  const averageSpeed = routes.vehicles.length
    ? Math.round(routes.vehicles.reduce((sum, truck) => sum + truck.speed, 0) / routes.vehicles.length)
    : 0;
  const onTimeRate = Math.round(
    routes.vehicles.length
      ? (routes.vehicles.filter((truck) => truck.status !== "delayed").length / routes.vehicles.length) * 100
      : 0,
  );

  const mapBounds = {
    minLat: 40.70,
    maxLat: 40.77,
    minLng: -74.03,
    maxLng: -73.92,
  };

  const toMapPoint = (point) => {
    const x = 24 + ((point.lng - mapBounds.minLng) / (mapBounds.maxLng - mapBounds.minLng || 1)) * 510;
    const y = 228 - ((point.lat - mapBounds.minLat) / (mapBounds.maxLat - mapBounds.minLat || 1)) * 180;
    return `${x},${y}`;
  };

  const tempChartPoints = buildChartPoints(temperatureTrend, 420, 180, 24);
  const tripChartPoints = buildChartPoints(tripTrend, 420, 180, 24);
  const tempLinePath = tempChartPoints.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const tripLinePath = tripChartPoints.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
  const tempAreaPath = `${tempLinePath} L ${tempChartPoints[tempChartPoints.length - 1].x} 156 L ${tempChartPoints[0].x} 156 Z`;
  const tripAreaPath = `${tripLinePath} L ${tripChartPoints[tripChartPoints.length - 1].x} 156 L ${tripChartPoints[0].x} 156 Z`;

  return (
    <main className="dashboard">
      <header className="header">
        <div>
          <p className="eyebrow">REAL-TIME FLEET INTELLIGENCE</p>
          <h1>Stream Forge</h1>
          <p className="subtitle">Truck telemetry pipeline monitoring dashboard</p>
        </div>
        <span className="status">{statusLabel}</span>
      </header>

      <section className="cards">
        <div className="card">
          <span>Active Trucks</span>
          <strong>{formatNumber(metrics.activeTrucks)}</strong>
        </div>

        <div className="card">
          <span>Events / Second</span>
          <strong>{formatNumber(metrics.eventsPerSecond)}</strong>
        </div>

        <div className="card">
          <span>Temperature Alerts</span>
          <strong>{metrics.temperatureAlerts}</strong>
        </div>

        <div className="card">
          <span>Fleet Uptime</span>
          <strong className="waiting">{metrics.fleetUptime.toFixed(1)}%</strong>
        </div>
      </section>

      <section className="topology-section">
        <div className="section-title">
          <div>
            <h2>Processing Topology</h2>
            <p>{liveSummary}</p>
          </div>
        </div>

        <div className="flow-container">
          <ReactFlow
            nodes={topology.nodes}
            edges={topology.edges}
            fitView
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
            proOptions={{ hideAttribution: true }}
          >
            <Background />
          </ReactFlow>
        </div>
      </section>

      <section className="kpi-panel">
        <div className="section-title">
          <div>
            <h2>Fleet KPI Dashboard</h2>
            <p>Route, thermal, and operational health summary</p>
          </div>
        </div>

        <div className="kpi-grid">
          <div className="kpi-box accent-blue">
            <span>Avg. Speed</span>
            <strong>{averageSpeed} km/h</strong>
            <small>Across active trucks</small>
          </div>
          <div className="kpi-box accent-green">
            <span>On-Time Rate</span>
            <strong>{onTimeRate}%</strong>
            <small>Route compliance</small>
          </div>
          <div className="kpi-box accent-amber">
            <span>Thermal Drift</span>
            <strong>{Math.max(...truckSummaries.map((truck) => truck.thermalVariance)).toFixed(1)}°C</strong>
            <small>Peak variance</small>
          </div>
          <div className="kpi-box accent-red">
            <span>Active Alerts</span>
            <strong>{truckSummaries.filter((truck) => truck.alerts > 0).length}</strong>
            <small>Route exceptions</small>
          </div>
        </div>

        <div className="fleet-toolbar" aria-label="Filter and sort trucks">
          <label className="search-field">
            <span className="sr-only">Search trucks</span>
            <span className="search-icon" aria-hidden="true">⌕</span>
            <input
              type="search"
              value={truckSearch}
              onChange={(event) => setTruckSearch(event.target.value)}
              placeholder="Search by truck name or ID"
            />
          </label>

          <label className="select-field">
            <span>Status</span>
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
              <option value="all">All trucks</option>
              <option value="moving">Moving</option>
              <option value="delayed">Delayed</option>
            </select>
          </label>

          <label className="select-field">
            <span>Sort by</span>
            <select value={sortOption} onChange={(event) => setSortOption(event.target.value)}>
              <option value="name">Truck name</option>
              <option value="speed">Speed: high to low</option>
              <option value="eta">ETA: soonest first</option>
              <option value="thermal">Thermal drift: high to low</option>
            </select>
          </label>

          <div className="fleet-result-count" aria-live="polite">
            <strong>{visibleTruckSummaries.length}</strong> of {truckSummaries.length} trucks
          </div>

          <button
            type="button"
            className="reset-filters"
            onClick={() => {
              setTruckSearch("");
              setStatusFilter("all");
              setSortOption("name");
            }}
            disabled={!hasActiveTruckFilters}
          >
            Reset
          </button>
        </div>

        <div className="summary-grid">
          {visibleTruckSummaries.map((truck) => (
            <article key={truck.id} className="summary-card">
              <div className="summary-header">
                <div>
                  <span className="summary-label">Truck</span>
                  <h3>{truck.name}</h3>
                </div>
                <span className={`summary-status ${truck.statusClass}`}>{truck.statusText}</span>
              </div>

              <div className="summary-metrics">
                <div>
                  <span>Speed</span>
                  <strong>{truck.speed} km/h</strong>
                </div>
                <div>
                  <span>ETA</span>
                  <strong>{truck.etaMinutes} min</strong>
                </div>
                <div>
                  <span>Temp drift</span>
                  <strong>{truck.thermalVariance}°C</strong>
                </div>
              </div>

              <div className="summary-progress">
                <div className="summary-progress-header">
                  <span>Route progress</span>
                  <strong>{truck.completion}%</strong>
                </div>
                <div className="progress-track">
                  <span style={{ width: `${truck.completion}%` }} />
                </div>
              </div>
            </article>
          ))}
          {visibleTruckSummaries.length === 0 && (
            <div className="empty-results">
              <strong>No trucks match these filters.</strong>
              <span>Try a different search or reset the filters.</span>
            </div>
          )}
        </div>
      </section>

      <section className="analytics-panel">
        <div className="section-title">
          <div>
            <h2>Analytics Graphs</h2>
            <p>Temperature drift and trip volume across the recent cycle</p>
          </div>
        </div>

        <div className="chart-grid">
          <article className="chart-card">
            <div className="chart-header">
              <div>
                <span className="chart-label">Temperature</span>
                <strong>Thermal drift</strong>
              </div>
              <span className="chart-tag tag-amber">+5.4°</span>
            </div>

            <svg viewBox="0 0 420 180" className="chart-svg" role="img" aria-label="Temperature analytics chart">
              <defs>
                <linearGradient id="tempGradient" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor="#fbbf24" stopOpacity="0.35" />
                  <stop offset="100%" stopColor="#fbbf24" stopOpacity="0.04" />
                </linearGradient>
              </defs>
              {[0, 1, 2, 3].map((step) => (
                <line key={`temp-grid-${step}`} x1="24" x2="396" y1={30 + step * 36} y2={30 + step * 36} stroke="#1e293b" strokeWidth="1" />
              ))}
              <path d={tempAreaPath} fill="url(#tempGradient)" />
              <path d={tempLinePath} fill="none" stroke="#fbbf24" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
              {tempChartPoints.map((point) => (
                <circle key={point.label} cx={point.x} cy={point.y} r="4" fill="#facc15" stroke="#fff" strokeWidth="2" />
              ))}
              {tempChartPoints.map((point) => (
                <text key={`temp-label-${point.label}`} x={point.x} y="170" textAnchor="middle" fill="#94a3b8" fontSize="10">
                  {point.label}
                </text>
              ))}
            </svg>
          </article>

          <article className="chart-card">
            <div className="chart-header">
              <div>
                <span className="chart-label">Trips</span>
                <strong>Completed</strong>
              </div>
              <span className="chart-tag tag-blue">+13.7%</span>
            </div>

            <svg viewBox="0 0 420 180" className="chart-svg" role="img" aria-label="Trips completed chart">
              <defs>
                <linearGradient id="tripGradient" x1="0" x2="0" y1="0" y2="1">
                  <stop offset="0%" stopColor="#38bdf8" stopOpacity="0.28" />
                  <stop offset="100%" stopColor="#38bdf8" stopOpacity="0.04" />
                </linearGradient>
              </defs>
              {[0, 1, 2, 3].map((step) => (
                <line key={`trip-grid-${step}`} x1="24" x2="396" y1={30 + step * 36} y2={30 + step * 36} stroke="#1e293b" strokeWidth="1" />
              ))}
              <path d={tripAreaPath} fill="url(#tripGradient)" />
              <path d={tripLinePath} fill="none" stroke="#38bdf8" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
              {tripChartPoints.map((point) => (
                <circle key={point.label} cx={point.x} cy={point.y} r="4" fill="#38bdf8" stroke="#fff" strokeWidth="2" />
              ))}
              {tripChartPoints.map((point) => (
                <text key={`trip-label-${point.label}`} x={point.x} y="170" textAnchor="middle" fill="#94a3b8" fontSize="10">
                  {point.label}
                </text>
              ))}
            </svg>
          </article>
        </div>
      </section>

      <section className="route-panel">
        <div className="section-title">
          <div>
            <h2>Movement Visualization</h2>
            <p>GPS route tracking and truck movement</p>
          </div>
        </div>

        <div className="route-layout">
          <div className="route-map">
            <svg viewBox="0 0 560 260" preserveAspectRatio="xMidYMid meet" role="img" aria-label="Truck movement map">
              <rect x="0" y="0" width="560" height="260" rx="14" fill="#0b1728" />
              <g opacity="0.18" stroke="#38bdf8" strokeWidth="1">
                {[...Array(8)].map((_, index) => (
                  <line key={`h-${index}`} x1="20" x2="540" y1={26 + index * 28} y2={26 + index * 28} />
                ))}
                {[...Array(8)].map((_, index) => (
                  <line key={`v-${index}`} x1={26 + index * 62} x2={26 + index * 62} y1="18" y2="242" />
                ))}
              </g>

              {routes.vehicles.map((truck) => {
                const pathPoints = truck.route.map((point) => toMapPoint(point)).join(" ");
                const currentPoint = { lat: truck.lat, lng: truck.lng };
                const currentPosition = toMapPoint(currentPoint).split(",").map(Number);

                return (
                  <g key={truck.id}>
                    <polyline
                      points={pathPoints}
                      fill="none"
                      stroke={truck.status === "delayed" ? "#f87171" : "#22c55e"}
                      strokeWidth="3"
                      strokeDasharray={truck.status === "delayed" ? "8 8" : "0"}
                      opacity="0.9"
                    />
                    <circle cx={currentPosition[0]} cy={currentPosition[1]} r="7" fill={truck.status === "delayed" ? "#f87171" : "#22c55e"} stroke="#dbeafe" strokeWidth="2" />
                    <text x={currentPosition[0] + 12} y={currentPosition[1] - 12} fill="#e2e8f0" fontSize="12" fontWeight="600">
                      {truck.name}
                    </text>
                  </g>
                );
              })}
            </svg>
          </div>

          <div className="route-legend">
            {routes.vehicles.map((truck) => (
              <div key={truck.id} className="route-item">
                <div className="route-header">
                  <span className="route-dot" style={{ background: truck.status === "delayed" ? "#f87171" : "#22c55e" }} />
                  <strong>{truck.name}</strong>
                </div>
                <div className="route-meta">
                  <span>{truck.status === "delayed" ? "Delayed" : "On schedule"}</span>
                  <span>{truck.speed} km/h</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>
    </main>
  );
}

export default App;