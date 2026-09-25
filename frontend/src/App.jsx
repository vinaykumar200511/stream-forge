import { useEffect, useState } from "react";
import { Background, Handle, Position, ReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import "./App.css";

const initialMetrics = {
  activeTrucks: 1248,
  eventsPerSecond: 13400,
  processingLagMs: 42,
  activeWorkers: 2,
  healthyWorkers: 2,
  kafkaStatus: "Healthy",
  temperatureAlerts: 12,
  fleetUptime: 99.2,
};

const fallbackHistory = {
  plate: "TS-29R2044",
  vehicle: "Truck 204",
  owner: "Northstar Cold Logistics",
  driver: "Maya Patel",
  driverStatus: "On duty",
  lastSeen: "2026-09-19 14:32",
  history: [
    { date: "2026-09-19", route: "Hyderabad to Mumbai", status: "Completed", distance: "48 km" },
    { date: "2026-09-18", route: "mumbai to hyderabad", status: "Completed", distance: "51 km" },
    { date: "2026-09-17", route: "Midtown Express", status: "Delayed", distance: "36 km" },
  ],
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
        { lat: 40.7000, lng: -74.01, timestamp: 1726752600 },
        { lat: 40.7060, lng: -74.02, timestamp: 1726752660 },
        { lat: 40.7128, lng: -74.006, timestamp: 1726752720 },
        { lat: 40.72, lng: -73.99, timestamp: 1726752780 },
      ],
    },
    {
      id: "truck-7",
      name: "Truck 7",
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

const StreamNode = ({ data, id }) => (
  <div className={`stream-node stream-node-${id} stream-node-${data.status || "unknown"}`}>
    <Handle type="target" position={Position.Left} />
    <div className="stream-node-heading">
      <span className="stream-node-dot" aria-hidden="true" />
      <strong>{data.label}</strong>
    </div>
    <span className="stream-node-status">{data.status || "unknown"}</span>
    {data.lag !== undefined && <small>{data.lag.toLocaleString()} lag</small>}
    {data.performance && <small>{Math.round(data.performance.throughput).toLocaleString()} ev/s · p95 {Math.round(data.performance.p95Latency)} ms</small>}
    <Handle type="source" position={Position.Right} />
  </div>
);

const nodeTypes = { stream: StreamNode };

const applyLiveMetricsToTopology = (currentTopology, liveMetrics) => {
  const group = liveMetrics?.groups?.[0];
  const partitions = (group?.topics || []).flatMap((topic) => topic.partitions || []);
  const totalLag = group?.totalLag || 0;
  const lagStatus = totalLag > 1000 ? "degraded" : liveMetrics?.status === "healthy" ? "healthy" : "unavailable";
  const processor = liveMetrics?.performance?.nodeStatuses?.find((item) => item.nodeId === "processor");
  return {
    ...currentTopology,
    nodes: currentTopology.nodes.map((node) => {
      if (node.id === "kafka") {
        return { ...node, data: { ...node.data, status: liveMetrics?.status === "healthy" ? "live" : "unavailable", lag: totalLag } };
      }
      if (node.id === "worker1") {
        return { ...node, data: { ...node.data, status: lagStatus, lag: partitions.filter((item) => item.partition <= 2).reduce((sum, item) => sum + item.lag, 0) } };
      }
      if (node.id === "worker2") {
        return { ...node, data: { ...node.data, status: lagStatus, lag: partitions.filter((item) => item.partition > 2).reduce((sum, item) => sum + item.lag, 0) } };
      }
      if (node.id === "aggregator" && processor) {
        return { ...node, data: { ...node.data, status: processor.status.toLowerCase(), performance: processor } };
      }
      return node;
    }),
  };
};

const filterOptions = {
  truckIds: ["truck-204", "truck-7", "truck-87", "truck-1", "truck-2", "truck-3", "truck-4", "truck-5", "truck-6"],
  dates: ["2026-09-19", "2026-09-18"],
  routes: ["Hudson Cold Chain", "Midtown Express", "Queens Transfer"],
  truckTypes: ["Refrigerated", "Frozen Goods", "Produce"],
};

const formatNumber = (value) =>
  new Intl.NumberFormat("en-US", {
    maximumFractionDigits: value >= 1000 ? 0 : 1,
  }).format(value);

const exportDataSet = (filename, rows, fields, format = "csv") => {
  if (!rows || !rows.length) {
    return;
  }

  let content = "";
  if (format === "json") {
    content = JSON.stringify(rows, null, 2);
  } else {
    const header = fields.join(",");
    const lines = rows.map((row) => {
      const values = fields.map((field) => {
        const rawValue = row[field] ?? "";
        const value = String(rawValue).replace(/"/g, '""');
        return `"${value}"`;
      });
      return values.join(",");
    });
    content = [header, ...lines].join("\n");
  }

  const blob = new Blob([content], {
    type: format === "json" ? "application/json;charset=utf-8" : "text/csv;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${filename}.${format}`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
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
  const [streamMetrics, setStreamMetrics] = useState({ status: "unavailable", partitions: [], totalLag: 0, maxPartitionLag: 0, consumerGroup: "streamforge-worker" });
  const [operations, setOperations] = useState({
    alerts: [],
    failures: { active: 0, today: 0, resolvedToday: 0, critical: 0, status: "loading" },
    temperature: { status: "loading", points: [] },
    throughput: { status: "loading", current: 0, average: 0, peak: 0, points: [] },
    trips: { status: "loading", items: [] },
    loads: { status: "loading", items: [] },
    deliveries: { status: "loading", items: [] },
  });
  const [trips, setTrips] = useState({ status: "loading", items: [] });
  const [performance, setPerformance] = useState({ throughputTests: [], alerts: [], activeAlerts: [], nodeStatuses: [], latestTest: null });
  const [topology, setTopology] = useState(buildFallbackTopology);
  const [routes, setRoutes] = useState(defaultRoutes);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [connectionState, setConnectionState] = useState("connecting");
  const [truckSearch, setTruckSearch] = useState("");
  const [truckIdFilter, setTruckIdFilter] = useState("all");
  const [dateFilter, setDateFilter] = useState("all");
  const [routeFilter, setRouteFilter] = useState("all");
  const [truckTypeFilter, setTruckTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortOption, setSortOption] = useState("name");
  const [selectedTruckId, setSelectedTruckId] = useState(null);
  const [plateSearch, setPlateSearch] = useState("NYC-204");
  const [plateHistory, setPlateHistory] = useState(fallbackHistory);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");

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
          nodes: data.nodes.map((node) => ({ ...node, type: "stream" })),
          edges: data.edges.map((edge) => ({
            ...edge,
            style: { stroke: "#38bdf8", strokeWidth: 2 },
          })),
        });
        setLastUpdated(new Date(data.updated_at * 1000));
        setConnectionState("live");
      } catch {
        if (!isMounted) {
          return;
        }
        setTopology(buildFallbackTopology());
        setLastUpdated(null);
        setConnectionState("fallback");
      }
    };

    const fetchMetrics = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/dashboard/metrics`);
        if (!response.ok) {
          throw new Error(`Metrics fetch failed: ${response.status}`);
        }

        const data = await response.json();
        if (isMounted && data?.metrics) {
          setMetrics(data.metrics);
        }
        if (isMounted && data?.streamMetrics) {
          setStreamMetrics(data.streamMetrics);
        }
      } catch {
        if (isMounted) {
          setConnectionState("fallback");
        }
      }
    };

    const fetchOperations = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/api/operations/overview`);
        if (!response.ok) throw new Error(`Operations fetch failed: ${response.status}`);
        const data = await response.json();
        if (isMounted) setOperations(data);
      } catch {
        if (isMounted) setOperations((current) => ({ ...current, status: "unavailable" }));
      }
    };

    const fetchTrips = async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/api/trips`);
        if (!response.ok) throw new Error(`Trips fetch failed: ${response.status}`);
        const data = await response.json();
        if (isMounted) setTrips(data);
      } catch {
        if (isMounted) setTrips({ status: "unavailable", items: [] });
      }
    };

    let metricsSocket;
    let reconnectTimer;
    const connectMetricsStream = () => {
      const socketUrl = `${API_BASE_URL.replace(/^http/, "ws")}/api/metrics/stream`;
      const socket = new WebSocket(socketUrl);
      metricsSocket = socket;
      socket.onopen = () => {
        if (isMounted) setConnectionState("live");
      };
      socket.onmessage = (event) => {
        if (!isMounted) return;
        let liveMetrics;
        try {
          liveMetrics = JSON.parse(event.data);
        } catch {
          return;
        }
        if (liveMetrics.operations) setOperations(liveMetrics.operations);
        if (liveMetrics.performance) setPerformance(liveMetrics.performance);
        const group = liveMetrics?.groups?.[0];
        const partitions = (group?.topics || []).flatMap((topic) => topic.partitions || []);
        setStreamMetrics({
          status: liveMetrics.status,
          consumerGroup: group?.consumerGroup || "streamforge-worker",
          partitions: partitions.map((item) => ({ partition: item.partition, consumerOffset: item.currentOffset, logEndOffset: item.latestOffset, lag: item.lag })),
          totalLag: group?.totalLag || 0,
          maxPartitionLag: Math.max(0, ...partitions.map((item) => item.lag)),
        });
        setTopology((currentTopology) => applyLiveMetricsToTopology(currentTopology, liveMetrics));
        setLastUpdated(new Date());
        setConnectionState(liveMetrics.status === "healthy" ? "live" : "fallback");
      };
      socket.onerror = () => socket.close();
      socket.onclose = () => {
        if (isMounted) {
          setConnectionState("fallback");
          reconnectTimer = window.setTimeout(connectMetricsStream, 2000);
        }
      };
      return socket;
    };

    const fetchRoutes = async () => {
      try {
        const params = new URLSearchParams();
        if (truckIdFilter !== "all") params.set("truck_id", truckIdFilter);
        if (dateFilter !== "all") params.set("date", dateFilter);
        if (routeFilter !== "all") params.set("route", routeFilter);
        if (truckTypeFilter !== "all") params.set("truck_type", truckTypeFilter);
        const query = params.toString();
        const response = await fetch(`${API_BASE_URL}/routes${query ? `?${query}` : ""}`);
        if (!response.ok) {
          throw new Error(`Route fetch failed: ${response.status}`);
        }

        const data = await response.json();
        if (isMounted && Array.isArray(data?.vehicles)) {
          setRoutes({ vehicles: data.vehicles });
          setConnectionState("live");
        }
      } catch {
        if (isMounted) {
          setRoutes(defaultRoutes);
          setConnectionState("fallback");
        }
      }
    };

    fetchTopology();
    fetchMetrics();
    fetchOperations();
    fetchTrips();
    fetchRoutes();
    connectMetricsStream();
    const topologyInterval = window.setInterval(fetchTopology, 4000);
    const routeInterval = window.setInterval(fetchRoutes, 5000);
    const metricsInterval = window.setInterval(fetchMetrics, 5000);
    const operationsInterval = window.setInterval(fetchOperations, 10000);
    const tripsInterval = window.setInterval(fetchTrips, 10000);

    return () => {
      isMounted = false;
      window.clearInterval(topologyInterval);
      window.clearInterval(routeInterval);
      window.clearInterval(metricsInterval);
      window.clearInterval(operationsInterval);
      window.clearInterval(tripsInterval);
      window.clearTimeout(reconnectTimer);
      metricsSocket?.close();
    };
  }, [dateFilter, routeFilter, truckIdFilter, truckTypeFilter]);

  const statusLabel = streamMetrics.status === "healthy" ? "System: Kafka Live" : "System: Kafka unavailable";
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
  const hasActiveTruckFilters = Boolean(normalizedSearch) || truckIdFilter !== "all" || dateFilter !== "all" || routeFilter !== "all" || truckTypeFilter !== "all" || statusFilter !== "all" || sortOption !== "name";
  const selectedTruck = truckSummaries.find((truck) => truck.id === selectedTruckId) || null;
  const searchPlateHistory = async (event) => {
    event.preventDefault();
    const normalizedPlate = plateSearch.trim().toUpperCase();
    if (!normalizedPlate) return;

    setHistoryLoading(true);
    setHistoryError("");
    try {
      const response = await fetch(`${API_BASE_URL}/vehicles/history?plate=${encodeURIComponent(normalizedPlate)}`);
      if (!response.ok) throw new Error("History lookup failed");
      const data = await response.json();
      if (data.status !== "ok" || !data.record) {
        setPlateHistory(null);
        setHistoryError(`No vehicle history found for ${normalizedPlate}.`);
        return;
      }
      setPlateHistory(data.record);
    } catch {
      setPlateHistory(normalizedPlate === fallbackHistory.plate ? fallbackHistory : null);
      setHistoryError(normalizedPlate === fallbackHistory.plate ? "Showing demo history while the API is unavailable." : "History service unavailable.");
    } finally {
      setHistoryLoading(false);
    }
  };
  const averageSpeed = routes.vehicles.length
    ? Math.round(routes.vehicles.reduce((sum, truck) => sum + truck.speed, 0) / routes.vehicles.length)
    : 0;
  const onTimeRate = Math.round(
    routes.vehicles.length
      ? (routes.vehicles.filter((truck) => truck.status !== "delayed").length / routes.vehicles.length) * 100
      : 0,
  );
  const delayedTruckCount = truckSummaries.filter((truck) => truck.status === "delayed").length;
  const movingTruckCount = truckSummaries.filter((truck) => truck.status === "moving").length;
  const fleetKpis = [
    { label: "Total trucks", value: truckSummaries.length, detail: "Reporting to the fleet feed", filter: "all" },
    { label: "In transit", value: movingTruckCount, detail: "Moving on active routes", filter: "moving" },
    { label: "Delayed", value: delayedTruckCount, detail: "Need operator attention", filter: "delayed" },
    { label: "Avg. speed", value: `${averageSpeed} km/h`, detail: "Across reporting trucks", filter: null },
  ];

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
        <div className="header-actions">
          <span className={`status status-${connectionState}`}>
            <span className="status-dot" aria-hidden="true" />
            {connectionState === "live" ? statusLabel : connectionState === "fallback" ? "Demo data mode" : "Connecting"}
          </span>
          <span className="alert-summary" aria-label={`${metrics.temperatureAlerts} temperature alerts`}>
            <span aria-hidden="true">!</span> {metrics.temperatureAlerts} alerts
          </span>
        </div>
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

      <section className="operations-grid" aria-label="Stream operations status">
        <div className="operation-card">
          <div className="operation-heading"><span className="operation-icon workers-icon">W</span><span>Worker health</span></div>
          <strong>{metrics.healthyWorkers}/{metrics.activeWorkers}</strong>
          <small>workers healthy</small>
          <div className="health-bar"><span style={{ width: `${metrics.activeWorkers ? (metrics.healthyWorkers / metrics.activeWorkers) * 100 : 0}%` }} /></div>
        </div>
        <div className="operation-card">
          <div className="operation-heading"><span className="operation-icon kafka-icon">K</span><span>Kafka status</span></div>
          <strong className="status-value">{metrics.kafkaStatus}</strong>
          <small>raw-telemetry · 6 partitions</small>
          <div className="status-line"><span className="status-dot" aria-hidden="true" /> Broker connection stable</div>
        </div>
        <div className="operation-card">
          <div className="operation-heading"><span className="operation-icon lag-icon">L</span><span>Processing lag</span></div>
          <strong>{metrics.processingLagMs}<small className="unit"> ms</small></strong>
          <small>consumer group p95</small>
          <div className="lag-track"><span style={{ width: `${Math.min(100, metrics.processingLagMs)}%` }} /></div>
        </div>
      </section>

      <section className="phase-two-grid" aria-label="Fleet operations">
        <article className="phase-two-panel alert-panel">
          <div className="section-title compact-title"><div><span className="chart-label">Temperature guardrails</span><h2>High-temperature alerts</h2></div><strong>{operations.alerts?.length || 0}</strong></div>
          {operations.alerts?.length ? operations.alerts.map((alert) => (
            <div className="alert-row" key={alert.deviceId}>
              <div><strong>{alert.deviceId}</strong><span>{alert.status} · {alert.durationSeconds}s</span></div>
              <b>{alert.temperature}°C <small> / {alert.threshold}°C</small></b>
            </div>
          )) : <p className="data-state">No active high-temperature alerts.</p>}
        </article>
        <article className="phase-two-panel">
          <div className="section-title compact-title"><div><span className="chart-label">Reliability</span><h2>Failure counters</h2></div><span className="source-state">{operations.failures?.status}</span></div>
          <div className="failure-grid">
            <div><strong>{operations.failures?.active ?? 0}</strong><span>Active</span></div>
            <div><strong>{operations.failures?.today ?? 0}</strong><span>Today</span></div>
            <div><strong>{operations.failures?.resolvedToday ?? 0}</strong><span>Resolved</span></div>
            <div><strong>{operations.failures?.critical ?? 0}</strong><span>Critical</span></div>
          </div>
        </article>
        <article className="phase-two-panel">
          <div className="section-title compact-title"><div><span className="chart-label">Throughput</span><h2>Event flow</h2></div><span className="source-state">{operations.throughput?.status}</span></div>
          {operations.throughput?.status === "available" ? <div className="throughput-values"><strong>{formatNumber(operations.throughput.current)} <small>events/s</small></strong><span>Average {formatNumber(operations.throughput.average)} · Peak {formatNumber(operations.throughput.peak)}</span></div> : <p className="data-state">Throughput data unavailable.</p>}
        </article>
        <article className="phase-two-panel phase-two-wide">
          <div className="section-title compact-title"><div><span className="chart-label">Operations</span><h2>Trips, loads, and deliveries</h2></div></div>
          <div className="trip-view-grid">
            <div className="trip-list">
              <div className="domain-status-heading"><span>Trip view</span><strong>{trips.items?.length || 0} active records</strong></div>
              {trips.items?.length ? trips.items.slice(0, 4).map((trip) => (
                <div className="trip-view-row" key={trip.tripId}><div><strong>{trip.vehicle}</strong><span>{trip.routeName || "Route history"} · {trip.status}</span></div><b>{trip.distanceKm} km</b></div>
              )) : <p className="data-state">{trips.status === "loading" ? "Loading trip data..." : "No trip data available."}</p>}
            </div>
            {["loads", "deliveries"].map((domain) => (
              <div key={domain} className="domain-status"><span>{domain}</span><strong>{operations[domain]?.status}</strong><small>{operations[domain]?.status === "unavailable" ? operations[domain]?.message : `${operations[domain]?.items?.length || 0} records`}</small></div>
            ))}
          </div>
        </article>
      </section>

      <section className="phase-three-panel" aria-label="Anomaly alerts and throughput testing">
        <div className="section-title">
          <div><span className="chart-label">Phase 3 observability</span><h2>Anomaly alerts and throughput tests</h2><p>Measured pipeline performance, bottleneck status, and persisted alert history.</p></div>
          <span className="source-state">{performance.latestTest ? `Last test ${new Date(performance.latestTest.completedAt).toLocaleTimeString()}` : "No test run"}</span>
        </div>
        <div className="phase-three-grid">
          <article className="anomaly-panel">
            <div className="panel-heading"><strong>Active anomalies</strong><span>{performance.activeAlerts.length}</span></div>
            {performance.activeAlerts.length ? performance.activeAlerts.map((alert) => <div className="anomaly-row" key={alert.id}><div><strong>{alert.alertType.replaceAll("_", " ")}</strong><span>{alert.nodeId} · {alert.description}</span></div><b>{alert.severity}</b></div>) : <p className="data-state">No active performance anomalies.</p>}
            <div className="panel-heading history-heading-small"><strong>Alert history</strong><span>{performance.alerts.length}</span></div>
            {performance.alerts.slice(0, 3).map((alert) => <div className="history-alert-row" key={alert.id}><span>{alert.alertType.replaceAll("_", " ")}</span><small>{alert.status} · {new Date(alert.triggeredAt).toLocaleTimeString()}</small></div>)}
          </article>
          <article className="throughput-test-panel">
            <div className="panel-heading"><strong>Throughput test result</strong><span>{performance.latestTest?.status || "NOT RUN"}</span></div>
            {performance.latestTest ? <div className="test-result-grid"><div><strong>{Math.round(performance.latestTest.actualRate).toLocaleString()}</strong><span>events/sec</span></div><div><strong>{performance.latestTest.p95LatencyMs}</strong><span>p95 ms</span></div><div><strong>{performance.latestTest.p99LatencyMs}</strong><span>p99 ms</span></div><div><strong>{performance.latestTest.failureRate * 100}%</strong><span>failure rate</span></div></div> : <p className="data-state">Run `POST /api/throughput-tests` to measure the pipeline.</p>}
            <div className="export-actions">
              <button type="button" className="export-button" disabled={!performance.alerts.length} onClick={() => exportDataSet("streamforge-alerts", performance.alerts, ["id", "alertType", "severity", "nodeId", "status", "triggeredAt"], "csv")}>CSV alerts</button>
              <button type="button" className="export-button" disabled={!performance.alerts.length} onClick={() => exportDataSet("streamforge-alerts", performance.alerts, ["id", "alertType", "severity", "nodeId", "status", "triggeredAt"], "json")}>JSON alerts</button>
              <button type="button" className="export-button" disabled={!performance.throughputTests.length} onClick={() => exportDataSet("streamforge-throughput", performance.throughputTests, ["id", "targetRate", "actualRate", "p95LatencyMs", "p99LatencyMs", "failureRate", "status", "completedAt"], "csv")}>CSV tests</button>
              <button type="button" className="export-button" disabled={!performance.throughputTests.length} onClick={() => exportDataSet("streamforge-throughput", performance.throughputTests, ["id", "targetRate", "actualRate", "p95LatencyMs", "p99LatencyMs", "failureRate", "status", "completedAt"], "json")}>JSON tests</button>
            </div>
            {performance.nodeStatuses.map((node) => <div className={`bottleneck-banner ${node.status.toLowerCase()}`} key={node.nodeId}><strong>{node.nodeId}: {node.status}</strong><span>{Math.round(node.throughput).toLocaleString()} ev/s · p95 {Math.round(node.p95Latency)} ms</span></div>)}
          </article>
        </div>
      </section>

      <section className="lag-panel" aria-label="Kafka partition lag">
        <div className="lag-panel-heading">
          <div>
            <span className="chart-label">Consumer group</span>
            <h2>{streamMetrics.consumerGroup}</h2>
          </div>
          <div className="lag-summary"><strong>{streamMetrics.totalLag.toLocaleString()}</strong><span>total lag</span></div>
          <div className="lag-summary"><strong>{streamMetrics.maxPartitionLag.toLocaleString()}</strong><span>peak partition lag</span></div>
        </div>
        <div className="partition-grid">
          {(streamMetrics.partitions || []).map((partition) => (
            <div className={`partition-cell ${partition.lag > 1000 ? "is-warning" : ""}`} key={partition.partition}>
              <span>Partition {partition.partition}</span>
              <strong>{partition.lag.toLocaleString()}</strong>
              <small>{partition.consumerOffset.toLocaleString()} / {partition.logEndOffset.toLocaleString()}</small>
            </div>
          ))}
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
            nodes={topology.nodes.map((node) => ({
              ...node,
              type: node.type || "stream",
              style: { ...node.style, padding: 0, background: "transparent", border: "none" },
            }))}
            edges={topology.edges}
            nodeTypes={nodeTypes}
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

        <div className="command-kpi-grid" aria-label="Fleet command center summary">
          {fleetKpis.map((kpi) => (
            <button
              key={kpi.label}
              type="button"
              className={`command-kpi ${kpi.filter && statusFilter === kpi.filter ? "is-active" : ""}`}
              onClick={() => {
                if (kpi.filter) {
                  setStatusFilter(kpi.filter);
                  setSortOption("name");
                  setTruckSearch("");
                }
              }}
              disabled={!kpi.filter}
            >
              <span>{kpi.label}</span>
              <strong>{kpi.value}</strong>
              <small>{kpi.detail}</small>
            </button>
          ))}
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

        <div className="history-panel">
          <div className="history-heading">
            <div>
              <span className="chart-label">Asset history</span>
              <h3>Number-plate search</h3>
              <p>Review ownership, assigned driver, and recent trips.</p>
            </div>
            <form className="plate-search" onSubmit={searchPlateHistory}>
              <label htmlFor="plate-search-input" className="sr-only">Search by number plate</label>
              <input id="plate-search-input" value={plateSearch} onChange={(event) => setPlateSearch(event.target.value)} placeholder="e.g. NYC-204" />
              <button type="submit" disabled={historyLoading}>{historyLoading ? "Searching..." : "Search plate"}</button>
            </form>
          </div>
          {historyError && <p className="history-message" role="status">{historyError}</p>}
          {plateHistory ? (
            <div className="history-content">
              <div className="owner-summary">
                <div className="plate-badge">{plateHistory.plate}</div>
                <div><strong>{plateHistory.vehicle}</strong><span>{plateHistory.owner}</span></div>
              </div>
              <div className="owner-facts">
                <div><span>Assigned driver</span><strong>{plateHistory.driver}</strong><small>{plateHistory.driverStatus}</small></div>
                <div><span>Last seen</span><strong>{plateHistory.lastSeen}</strong><small>Telemetry timestamp</small></div>
              </div>
              <div className="trip-history">
                <div className="history-table-header"><span>Recent trip history</span><span>{plateHistory.history.length} records</span></div>
                {plateHistory.history.map((trip) => (
                  <div className="history-row" key={`${trip.date}-${trip.route}`}><span>{trip.date}</span><strong>{trip.route}</strong><span className={trip.status === "Delayed" ? "trip-delayed" : "trip-complete"}>{trip.status}</span><span>{trip.distance}</span></div>
                ))}
              </div>
            </div>
          ) : <div className="history-empty">No history available for this plate.</div>}
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
            <span>Truck ID</span>
            <select value={truckIdFilter} onChange={(event) => setTruckIdFilter(event.target.value)}>
              <option value="all">All truck IDs</option>
              {filterOptions.truckIds.map((truckId) => <option key={truckId} value={truckId}>{truckId}</option>)}
            </select>
          </label>

          <label className="select-field">
            <span>Date</span>
            <select value={dateFilter} onChange={(event) => setDateFilter(event.target.value)}>
              <option value="all">All dates</option>
              {filterOptions.dates.map((date) => <option key={date} value={date}>{date}</option>)}
            </select>
          </label>

          <label className="select-field">
            <span>Route</span>
            <select value={routeFilter} onChange={(event) => setRouteFilter(event.target.value)}>
              <option value="all">All routes</option>
              {filterOptions.routes.map((route) => <option key={route} value={route}>{route}</option>)}
            </select>
          </label>

          <label className="select-field">
            <span>Truck type</span>
            <select value={truckTypeFilter} onChange={(event) => setTruckTypeFilter(event.target.value)}>
              <option value="all">All types</option>
              {filterOptions.truckTypes.map((truckType) => <option key={truckType} value={truckType}>{truckType}</option>)}
            </select>
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
              setTruckIdFilter("all");
              setDateFilter("all");
              setRouteFilter("all");
              setTruckTypeFilter("all");
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
            <article
              key={truck.id}
              className={`summary-card ${selectedTruckId === truck.id ? "is-selected" : ""}`}
              onClick={() => setSelectedTruckId(truck.id)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  setSelectedTruckId(truck.id);
                }
              }}
              role="button"
              tabIndex="0"
              aria-label={`View details for ${truck.name}`}
            >
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

        {selectedTruck && (
          <aside className="truck-detail" aria-label={`${selectedTruck.name} details`}>
            <div className="truck-detail-header">
              <div>
                <span className="summary-label">Selected asset</span>
                <h3>{selectedTruck.name}</h3>
              </div>
              <button type="button" className="detail-close" onClick={() => setSelectedTruckId(null)} aria-label="Close truck details">×</button>
            </div>
            <div className="truck-detail-grid">
              <div><span>Status</span><strong>{selectedTruck.statusText}</strong></div>
              <div><span>Current speed</span><strong>{selectedTruck.speed} km/h</strong></div>
              <div><span>ETA</span><strong>{selectedTruck.etaMinutes} min</strong></div>
              <div><span>Thermal drift</span><strong>{selectedTruck.thermalVariance}°C</strong></div>
              <div><span>Coordinates</span><strong>{selectedTruck.lat.toFixed(4)}, {selectedTruck.lng.toFixed(4)}</strong></div>
              <div><span>Telemetry</span><strong>{connectionState === "live" ? "Live feed" : "Demo feed"}</strong></div>
            </div>
            <p className="detail-note">Driver, shipment, and compliance records are not part of the current telemetry API.</p>
          </aside>
        )}
      </section>

      <section className="analytics-panel">
        <div className="section-title">
          <div>
            <h2>Telemetry analytics</h2>
            <p>Charts are driven by backend history sources; unavailable histories are shown explicitly.</p>
          </div>
        </div>
        <div className="chart-grid">
          <article className="chart-card source-chart">
            <div className="chart-header"><div><span className="chart-label">Temperature</span><strong>Historical readings</strong></div><span className="chart-tag tag-amber">{operations.temperature?.status}</span></div>
            <div className="chart-empty"><strong>{operations.temperature?.status === "unavailable" ? "No temperature history" : "Loading temperature history"}</strong><span>{operations.temperature?.message || "Select a device and time range when historical samples are available."}</span><small>Threshold: {operations.temperature?.threshold ?? "-"}°C</small></div>
          </article>
          <article className="chart-card source-chart">
            <div className="chart-header"><div><span className="chart-label">Throughput</span><strong>Events per second</strong></div><span className="chart-tag tag-blue">{operations.throughput?.status}</span></div>
            <div className="throughput-chart-value"><strong>{formatNumber(operations.throughput?.current || 0)}</strong><span>current events/s</span><small>Average {formatNumber(operations.throughput?.average || 0)} · Peak {formatNumber(operations.throughput?.peak || 0)}</small></div>
            <div className="chart-empty compact"><span>{operations.throughput?.points?.length ? "Historical throughput available." : "Historical throughput is not persisted yet."}</span></div>
          </article>
        </div>
      </section>

      <section className="route-panel">
        <div className="section-title">
          <div>
            <h2>GPS Route History</h2>
            <p>Live position, route breadcrumbs, and movement history</p>
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
                    {selectedTruckId === truck.id && <polyline points={pathPoints} fill="none" stroke="#f8fafc" strokeWidth="1" opacity="0.8" />}
                    <circle
                      cx={currentPosition[0]}
                      cy={currentPosition[1]}
                      r="7"
                      fill={truck.status === "delayed" ? "#f87171" : "#22c55e"}
                      stroke="#dbeafe"
                      strokeWidth="2"
                      onClick={() => setSelectedTruckId(truck.id)}
                      role="button"
                      tabIndex="0"
                    />
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
              <button key={truck.id} type="button" className={`route-item ${selectedTruckId === truck.id ? "is-selected" : ""}`} onClick={() => setSelectedTruckId(truck.id)}>
                <div className="route-header">
                  <span className="route-dot" style={{ background: truck.status === "delayed" ? "#f87171" : "#22c55e" }} />
                  <strong>{truck.name}</strong>
                </div>
                <div className="route-meta">
                  <span>{truck.status === "delayed" ? "Delayed" : "On schedule"}</span>
                  <span>{truck.speed} km/h</span>
                </div>
              </button>
            ))}
          </div>

          <div className="route-history">
            <div className="route-history-heading">
              <div>
                <span className="chart-label">Breadcrumb history</span>
                <h3>{selectedTruck ? selectedTruck.name : "Select a truck"}</h3>
              </div>
              {selectedTruck && <span className={`summary-status ${selectedTruck.statusClass}`}>{selectedTruck.statusText}</span>}
            </div>
            {selectedTruck ? (
              <>
                <div className="route-history-meta"><span>{selectedTruck.route_name || "Active route"}</span><strong>{selectedTruck.speed} km/h</strong></div>
                <ol className="breadcrumb-list">
                  {selectedTruck.route.map((point, index) => (
                    <li key={`${point.lat}-${point.lng}`} className={index === selectedTruck.route.length - 1 ? "is-current" : ""}>
                      <span>Point {index + 1}</span>
                      <strong>{point.lat.toFixed(4)}, {point.lng.toFixed(4)}</strong>
                      {point.timestamp && <small>{new Date(point.timestamp * 1000).toLocaleTimeString()}</small>}
                      {index === selectedTruck.route.length - 1 && <small>Current GPS position</small>}
                    </li>
                  ))}
                </ol>
              </>
            ) : <p className="history-empty">Choose a truck on the map or in the list to inspect its GPS route.</p>}
          </div>
        </div>
      </section>
    </main>
  );
}

export default App;