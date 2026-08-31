import { Background, Controls, MiniMap, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import "./App.css";

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
  return (
    <main className="dashboard">
      <header className="header">
        <div>
          <p className="eyebrow">REAL-TIME FLEET INTELLIGENCE</p>
          <h1>Stream Forge</h1>
          <p className="subtitle">
            Truck telemetry pipeline monitoring dashboard
          </p>
        </div>
        <span className="status">System: Waiting for Kafka</span>
      </header>

      <section className="cards">
        <div className="card">
          <span>Active Trucks</span>
          <strong>0</strong>
        </div>

        <div className="card">
          <span>Events / Second</span>
          <strong>0</strong>
        </div>

        <div className="card">
          <span>Temperature Alerts</span>
          <strong>0</strong>
        </div>

        <div className="card">
          <span>Kafka Status</span>
          <strong className="waiting">Waiting</strong>
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
          <ReactFlow nodes={nodes} edges={edges} fitView>
            <Background />
            <Controls />
            <MiniMap />
          </ReactFlow>
        </div>
      </section>
    </main>
  );
}

export default App;