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

    fetchTopology();
    const interval = window.setInterval(fetchTopology, 4000);

    return () => {
      isMounted = false;
      window.clearInterval(interval);
    };
  }, []);

  const statusLabel = metrics.eventsPerSecond > 10000 ? "System: Kafka Live" : "System: Stable";
  const liveSummary = lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : "Fallback topology";

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
    </main>
  );
}

export default App;