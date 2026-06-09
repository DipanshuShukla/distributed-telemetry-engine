# Distributed Industrial Telemetry Engine

An enterprise-grade, event-driven data ingestion and anomaly detection pipeline designed to monitor high-frequency industrial proxy metrics (Thermal, Energy, and Safety). Built to demonstrate high-throughput stream processing, stateful signal debouncing, and sub-second dashboard visualization.

## 🏗️ Architecture Overview

The system is decoupled into discrete microservices communicating via an Apache Kafka event backbone. It prioritizes network I/O efficiency through micro-batching and leverages Java 21 Virtual Threads for concurrent partition consumption.

### System Data Flow
1. **Generator Service:** Simulates edge-device sensors. Injects realistic noise/jitter and fires JSON telemetry events into a partitioned Kafka topic.
2. **Message Broker (Kafka):** Queues incoming data using a 3-partition topology for horizontal consumer scaling.
3. **Processor Service:** Consumes batches concurrently. Executes a rules-based inference engine to detect faults. 
4. **Metrics Cache (Redis):** Handles stateful anomaly debouncing (preventing false positives), tracks active asset heartbeats, and maintains ultra-low-latency $O(1)$ snapshots for the frontend.
5. **Persistence (PostgreSQL):** Stores the historical logs of validated anomalies via efficient JDBC bulk-inserts.
6. **Control Dashboard:** A vanilla JS frontend polling the Spring Boot REST API to render live telemetry and network health without stressing the relational database.

## 🛠️ Technology Stack

* **Core Backend:** Java 21, Spring Boot 3.2, Spring Data JPA, Spring Kafka
* **Message Broker:** Apache Kafka 3.6 (KRaft Mode - ZooKeeper-less)
* **High-Speed Cache & State:** Redis 7 (ZSets and Hashes)
* **Relational Storage:** PostgreSQL 15
* **Orchestration:** Docker & Docker Compose V2
* **Visualization:** HTML5, Vanilla JavaScript, Chart.js

## 🧠 Key Architectural Decisions & Trade-offs

### 1. High-Throughput Micro-Batching
Processing thousands of events per second one-by-one crushes database connection pools. The consumer is configured to poll `List<ConsumerRecord>` in chunks. This transforms individual network trips to PostgreSQL into a single bulk `INSERT`, allowing a single local node to comfortably process ~3,500+ EPS.

### 2. Stateful Signal Debouncing (Anti-Alarm Fatigue)
Industrial sensors vibrate and produce dirty data. To prevent false-positive anomaly alerts, the system uses Redis to track consecutive threshold breaches. An anomaly is only logged if the sensor breaches the safety limit for **3 consecutive readings**, acting as a distributed debounce filter.
* **Trade-off:** Why not store this state in Java memory? Because in a scaled environment with multiple processor instances, state must be centralized. Redis handles this with sub-millisecond latency.

### 3. $O(1)$ Live Chart Snapshotting vs. WebSockets
Rather than querying the SQL database for live chart data or establishing heavy WebSocket connections for thousands of events, the processor updates a single Redis key with the *most critical* reading from the current batch. 
* **Trade-off:** While WebSockets offer true real-time pushing, a 1-second REST polling interval against a flat Redis String provides a highly resilient, visually identical "live" experience while drastically reducing server memory overhead and architectural complexity.

### 4. Client-Side Heartbeat Monitoring
Redis tracks active machines using a timestamp. Instead of running heavy background cron jobs on the server to delete offline machines, the API simply passes the timestamp to the frontend. The client evaluates `(CurrentTime - LastSeen) < 10s` to instantly render devices as Online or Offline.

## 📋 Assumptions & Constraints

Given the limited time-box for this assignment, the following pragmatic assumptions were made:
* **Simulated Inference:** Real-world ML anomaly detection requires heavy Python/TensorFlow models. For this assignment, the "AI Engine" is mocked using strict, physics-based conditional thresholds applied to the telemetry stream.
* **Security & Auth:** JWT authentication and SSL termination were omitted to focus entirely on the distributed systems and data engineering challenges.
* **Single-Node Deployment:** While the `docker-compose.yml` simulates a multi-node network locally, production would utilize Kubernetes (EKS/GKE) for true isolation.

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose (V2)
* Ports `80`, `8080`, `5432`, `6379`, `9092` available on your local host.

### Launching the Stack
The entire infrastructure and microservices are containerized. From the root directory, execute:

```bash
# Build the microservices and launch the infrastructure
docker compose up -d --build

```

### Viewing the Application

* **Industrial Dashboard:** Open `http://localhost/index.html` (via Nginx Proxy)
* **Service APIs:**
* Metrics: `GET http://localhost/api/telemetry/cluster-metrics`
* Anomalies: `GET http://localhost/api/telemetry/recent-anomalies`



## 📈 Scaling the Cluster (Live Demonstration)

The architecture is designed to scale horizontally across the Kafka partitions. To demonstrate elastic scaling, use Docker Compose to increase the processing nodes and crank up the ingestion rate:

```bash
PROCESSOR_REPLICAS=3 GENERATOR_RATE=200 docker compose up -d

```

*Note: The Kafka topic `raw-telemetry` is pre-configured with 3 partitions to support up to 3 concurrent worker threads/instances.*

## 🔮 Future Production Scope

Given a longer development runway, the next immediate architectural upgrades would be:

1. **Infrastructure Monitoring (Consumer Lag):** Deploying Prometheus and Grafana with Kafka JMX exporters to strictly monitor partition offsets, consumer lag, and producer ingress rates at the infrastructure level.
2. **Service Discovery:** Integrating **HashiCorp Consul** to dynamically register processor nodes as they scale up and down.
3. **Time-Series Database:** Migrating historical logs from standard PostgreSQL to TimescaleDB or InfluxDB for highly optimized time-window querying.