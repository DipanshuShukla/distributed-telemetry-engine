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

* **Core Backend:** Java 27, Spring Boot 3.2, Spring Data JPA, Spring Kafka
* **Message Broker:** Apache Kafka 3.6 (KRaft Mode - ZooKeeper-less)
* **High-Speed Cache & State:** Redis 7 (ZSets and Hashes)
* **Relational Storage:** PostgreSQL 15
* **Orchestration:** Docker & Docker Compose V2
* **Visualization:** HTML5, Vanilla JavaScript, Chart.js

## 🧠 Key Architectural Decisions

### 1. High-Throughput Micro-Batching
Processing thousands of events per second one-by-one crushes database connection pools. The consumer is configured to poll `List<ConsumerRecord>` in chunks of 500. This transforms 500 individual network trips to PostgreSQL into a single bulk `INSERT`, allowing a single local node to comfortably process ~1,000 EPS.

### 2. Stateful Signal Debouncing (Anti-Alarm Fatigue)
Industrial sensors vibrate and produce dirty data. To prevent false-positive anomaly alerts, the system uses Redis to track consecutive threshold breaches. An anomaly is only logged to the database if the sensor breaches the safety limit for **3 consecutive readings**, acting as a distributed debounce filter.

### 3. $O(1)$ Live Chart Snapshotting (Metrics Masking Prevention)
Rather than querying the SQL database for live chart data or sending thousands of events over HTTP, the processor updates a single Redis key with the *most critical* reading from the current batch. This decouples the visualization layer from the ingestion volume, keeping dashboard rendering lightweight and instant.

### 4. Client-Side Heartbeat Monitoring
Redis tracks active machines using a timestamp. Instead of running heavy background cron jobs to delete offline machines, the API simply passes the timestamp to the frontend. The client evaluates `(CurrentTime - LastSeen) < 10s` to instantly render devices as Online or Offline.

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose (V2)
* Java 21 (Optional, if running services outside of Docker)

### Launching the Stack
The entire infrastructure and microservices are containerized. From the root directory, execute:

```bash
# Build the microservices and launch the infrastructure
docker compose up -d --build

```

### Viewing the Application

* **Industrial Dashboard:** Open `http://localhost:8080/index.html` in your browser.
* **Service APIs:**
* Metrics: `GET http://localhost:8080/api/telemetry/cluster-metrics`
* Anomalies: `GET http://localhost:8080/api/telemetry/recent-anomalies`



## 📈 Scaling the Cluster (Live Demonstration)

The architecture is designed to scale horizontally across the Kafka partitions. To demonstrate elastic scaling, use Docker Compose to increase the processing nodes and crank up the ingestion rate:

```bash
PROCESSOR_REPLICAS=3 GENERATOR_RATE=200 docker compose up -d

```

*Note: The Kafka topic `raw-telemetry` is pre-configured with 3 partitions to support up to 3 concurrent worker threads/instances.*

## 🔮 Future Production Scope

Given a longer development runway, the next immediate architectural upgrades would be:

1. **Service Discovery:** Integrating **HashiCorp Consul** (`spring-cloud-starter-consul-discovery`) to dynamically register processor nodes as they scale up and down.
2. **API Gateway:** Deploying an NGINX Reverse Proxy or Spring Cloud Gateway to act as a single entry point and load-balance frontend REST requests across the active processor replicas.
3. **Time-Series Database:** Migrating historical logs from standard PostgreSQL to TimescaleDB or InfluxDB for optimized time-window querying.
