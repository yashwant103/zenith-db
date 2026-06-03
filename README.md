Markdown
# ZenithDB

A high-performance, fault-tolerant distributed key-value store built from scratch in **Java 21**. ZenithDB implements the **Raft consensus algorithm** to guarantee strong linearizable consistency and utilizes a non-blocking **Java NIO** reactor pattern to handle heavy concurrent workloads without thread-per-connection overhead.

## 🚀 Core Features & Architecture

* **Raft Consensus Engine:** Custom implementation of leader election (with randomized timeouts to prevent split votes), log replication, and safe atomic log compaction. Handles minority node failures gracefully.
* **Non-Blocking Network I/O:** Built on a Java NIO single-threaded event loop (`Selector` + `SocketChannel`) multiplexing both Raft inter-node RPCs (Jackson polymorphic JSON) and plain-text client commands. Sustains **10,000+ OPs/sec**.
* **High-Durability Write-Ahead Log (WAL):** Batched WAL with group-commit semantics (`ConcurrentLinkedQueue` + `ScheduledExecutorService`) syncing 500 entries per batch. Atomic file renames reduce recovery log sizes by up to **90%**.
* **Lock-Free Storage Engine:** In-memory storage powered by `ConcurrentHashMap` and CPU-level Compare-And-Swap (CAS) atomic updates. Integrates a 100k-entry LRU idempotency cache to eliminate duplicate transactions during network retries.
* **Full-Stack Observability:** Zero-dependency, embedded Prometheus metrics endpoint exposing real-time cluster health. Pre-provisioned Grafana dashboards track an average consensus latency of **13.3ms**.
* **Containerized Deployment:** Docker multi-stage builds compile the engine into a heavily stripped-down **50MB Alpine JRE image**. Deployed via `docker-compose` utilizing named volumes for persistent WAL storage across restarts.

## 🛠️ Prerequisites

* [Docker](https://docs.docker.com/get-docker/) and `docker-compose`
* Java 21 (for local compilation/testing)
* Maven 3.8+

## 📦 Getting Started

### 1. Clone the Repository
```bash
git clone [https://github.com/yashwant3/ZenithDB.git](https://github.com/yashwant3/ZenithDB.git)
cd ZenithDB
2. Bootstrapping the Cluster
ZenithDB is fully containerized. You can spin up the 3-node cluster and the entire observability stack (Prometheus + Grafana) with a single command:

Bash
# Builds the 50MB multi-stage Docker images and starts the cluster
docker-compose up -d --build
3. Accessing Observability (Grafana)
Once the cluster boots and a leader is elected, you can monitor the real-time metrics:

Navigate to http://localhost:3000 in your browser.

The ZenithDB cluster dashboard is auto-provisioned. You will see gauges for Trades Total, Active Trades, Raft Term, and Leader Status.

⚡ Interacting with ZenithDB (Plain-Text Client)
ZenithDB's NIO server accepts plain-text commands. You can interact with the database directly using netcat (nc) or telnet on the exposed client port (e.g., 8080).

Set a Key-Value Pair:

Bash
echo 'SET user:1001 {"balance": 5000}' | nc localhost 8080
Expected Output: ACK (after Raft majority consensus is reached and WAL is flushed)

Get a Value:

Bash
echo 'GET user:1001' | nc localhost 8080
Expected Output: {"balance": 5000}

🧪 Testing & Fault Tolerance Validation
ZenithDB includes a rigorous suite of JUnit 5 chaos tests to validate network partitions and crash recovery.

To run the test suite locally:

Bash
mvn clean test
Manual Chaos Testing
You can manually simulate a leader crash to observe the randomized timeout election process:

Identify the current leader via the Grafana dashboard.

Kill the leader container: docker kill zenith-node-1

Watch the Grafana dashboard: the remaining two nodes will trigger an election timeout and establish a new leader within ~1850ms, resuming normal read/write operations without data loss.

📜 System Invariants Maintained
Consistency (CP over AP): ZenithDB prioritizes Consistency and Partition Tolerance. If a network partition leaves a node without a quorum, it will reject writes rather than serve stale or divergent data.

Idempotency: The 100k-entry LRU cache guarantees that if a client re-submits a write command due to a dropped network ACK, the state machine will not apply the mutation twice.

👨‍💻 Author
Yashwant Jadhao

LinkedIn: yashwant-jadhao

Codeforces: brruteeForcebabe
