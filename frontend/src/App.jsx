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

const formatNumber = (value) =>
  new Intl.NumberFormat("en-US", {
    maximumFractionDigits: value >= 1000 ? 0 : 1,
  }).format(value);

const nodes = [
  {
    id: "producer",
    position: { x: 60, y: 180 },
    data: { label: "Truck Telemetry Producer" },
    style: {
      background: "#2563eb",
      color: "#ffffff",
      border: "none",
      borderRadius: "8px",
      padding: "12px",
    },
  },
  {
    id: "kafka",
    position: { x: 330, y: 180 },
    data: { label: "Kafka: truck-telemetry" },
    style: {
      background: "#f97316",
      color: "#ffffff",
      border: "none",
      borderRadius: "8px",
      padding: "12px",
    },
  },
  {
    id: "processor",
    position: { x: 620, y: 180 },
    data: { label: "Stream Processor" },
    style: {
      background: "#16a34a",
      color: "#ffffff",
      border: "none",
      borderRadius: "8px",
      padding: "12px",
    },
  },
  {
    id: "dashboard",
    position: { x: 900, y: 180 },
    data: { label: "Fleet Dashboard" },
    style: {
      background: "#7c3aed",
      color: "#ffffff",
      border: "none",
      borderRadius: "8px",
      padding: "12px",
    },
  },
];

const edges = [
  { id: "producer-kafka", source: "producer", target: "kafka", animated: true },
  { id: "kafka-processor", source: "kafka", target: "processor", animated: true },
  { id: "processor-dashboard", source: "processor", target: "dashboard", animated: true },
];

function App() {
  const [metrics, setMetrics] = useState(initialMetrics);

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

  const statusLabel = metrics.eventsPerSecond > 10000 ? "System: Kafka Live" : "System: Stable";

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
            <p>Static Week 1 pipeline view</p>
          </div>
        </div>

        <div className="flow-container">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            fitView
            proOptions={{ hideAttribution: true }}
          >
            <Background />
          </ReactFlow>
        </div>
      </section>
    </main>
  );
}

export default App;