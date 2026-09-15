# StreamForge — Repository Folder Structure & Modular Blueprint

```text
Streamforge/
├── .env                              # Environment variable configuration
├── .env.example                      # Example environment template
├── .gitignore                        # Git ignore rules for Python, node_modules, IDEs
├── docker-compose.yml                # Kafka KRaft cluster (6 partitions) + Kafka-UI
├── requirements.txt                  # Python dependency specifications
├── README.md                         # Main architectural documentation & quickstart
│
├── docs/                             # Architecture, specifications & review evidence
│   ├── PROJECT_SCOPE.md              # Business context, functional SLAs & goals
│   ├── SCHEMA_PLAN.md                # IoT message format, JSON schemas & partition keys
│   ├── FOLDER_STRUCTURE.md           # Repository architectural layout
│   ├── DEMO_SCRIPT.md                # Pitch & live demonstration instructions
│   └── evidence/                     # Benchmarks, logs & resilience test outputs
│       ├── chaos_failover_log.md     # Proof of 0.00% state loss on worker kill
│       └── throughput_benchmark.md   # Proof of >13k ev/s benchmark
│
├── streamforge/                      # Core Python source package
│   ├── __init__.py                   # Package root
│   │
│   ├── common/                       # Shared modules & domain models
│   │   ├── __init__.py               # Public exports
│   │   ├── config.py                 # Pydantic BaseSettings (.env loader)
│   │   ├── models.py                 # Pydantic v2 domain models (Raw, Aggregate, Alert)
│   │   └── kafka_admin.py            # Topic creation & cluster provisioning tool
│   │
│   ├── producer/                     # Ingestion Tier (IoT simulation)
│   │   ├── __init__.py
│   │   ├── simulator.py              # Multi-tenant thermodynamic fleet simulator
│   │   └── producer.py               # Async aiokafka high-throughput publisher
│   │
│   ├── processor/                    # Stream Processing Tier (Stateful windowing)
│   │   ├── __init__.py
│   │   ├── worker.py                 # Distributed stream processor worker
│   │   ├── window_manager.py         # 5-minute tumbling/sliding window aggregator
│   │   └── state_store.py            # RocksDB local state store & changelog sync
│   │
│   ├── alerts/                       # Alerting & Incident Dispatch Tier
│   │   ├── __init__.py
│   │   ├── anomaly_engine.py         # Real-time threshold breach detector
│   │   └── webhook_dispatcher.py     # Async HTTP webhook notification dispatcher
│   │
│   └── backend/                      # API & Observability Tier
│       ├── __init__.py
│       ├── app.py                    # FastAPI application entrypoint
│       ├── auth.py                   # JWT security & multi-tenant auth layer
│       ├── websocket.py              # Live WebSocket broadcast manager
│       └── metrics.py                # Prometheus /metrics exporter
│
├── frontend/                         # Real-Time React Flow Dashboard
│   ├── package.json                  # Node.js dependencies
│   ├── vite.config.js                # Vite build config
│   ├── src/
│   │   ├── App.jsx                   # Main React Flow pipeline diagram & graphs
│   │   ├── components/               # Custom React Flow nodes & charts
│   │   └── index.css                 # Premium dark-mode glassmorphism styling
│
└── tests/                            # Automated Verification Test Suite
    ├── __init__.py
    ├── test_models.py                # Unit tests for domain models & serialization
    ├── test_simulator.py             # Unit tests for fleet simulator & anomalies
    ├── test_processor.py             # Unit tests for window aggregation & grace period
    ├── test_auth.py                  # Unit tests for JWT security & RBAC
    ├── chaos_test.py                 # Automated worker kill-and-recover verification
    └── load_test.py                  # High-throughput benchmark script
```
