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
  ],
};

const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

const formatNumber = (value) =>
  new Intl.NumberFormat("en-US", {
    maximumFractionDigits: value >= 1000 ? 0 : 1,
  }).format(value);

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