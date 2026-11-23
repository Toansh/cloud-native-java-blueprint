# ADR-003: Transactional Outbox Pattern & Distributed Event-Driven Architecture

## Metadata
- **Status**: Accepted
- **Author**: Ansh Taneja
- **Date**: 2024-Q4
- **Context**: `cloud-native-java-blueprint` (Java 21, Spring Boot 3.3.5, Apache Kafka, PostgreSQL)

---

## 1. Context & The Dual-Write Problem

In a distributed microservice architecture, state transitions inside a service boundary frequently require two atomic actions:
1. Mutating relational domain state within an ACID database (PostgreSQL).
2. Publishing an event notification to a distributed streaming broker (Apache Kafka) so downstream consumers can synchronize state asynchronously.

Directly executing both operations in application code introduces the fundamental **Dual-Write Problem**:
- If the application commits the database transaction first and crashes (or encounters a network failure) before publishing to Kafka, the message is permanently lost, causing severe cross-service data drift.
- Conversely, if the application publishes to Kafka first and the subsequent database commit rolls back (due to database constraint violations, deadlock, or timeout), downstream consumers process a "phantom event" representing state that was never persisted.

Because relational databases and Kafka brokers operate on separate consensus domains, standard atomic commitments cannot span both systems without specialized coordination.

```
       [ Client Request ]
               │
               ▼
   ┌───────────────────────┐
   │  Spring Boot Service  │
   └───┬───────────────┬───┘
       │ (1) COMMIT    │ (2) PRODUCE (Broker down / SIGTERM crash)
       ▼               ▼
 ┌───────────┐   ┌───────────┐
 │ PostgreSQL│   │   Kafka   │ ===> [ Message Lost / State Drift ]
 └───────────┘   └───────────┘
```

---

## 2. Options Evaluated

### Option A: Two-Phase Commit (2PC / XA Distributed Transactions)
- **Mechanism**: Use an XA coordinator (e.g., Narayana, Atomikos) to orchestrate a two-phase commit across PostgreSQL and the messaging broker.
- **Verdict**: **Rejected**.
- **Rationale**: 
  - Apache Kafka does not natively participate in XA/2PC protocols.
  - 2PC is a blocking, synchronous protocol that degrades throughput and availability.
  - An unresponsive coordinator locks database rows, violating cloud-native horizontal scalability principles.

### Option B: Naive Publish-After-Commit
- **Mechanism**: Execute the database transaction in `@Transactional`, register a Spring `TransactionSynchronization.afterCommit()` hook, and publish to Kafka upon successful commit.
- **Verdict**: **Rejected**.
- **Rationale**:
  - Does not protect against JVM SIGKILL, pod eviction (`OOMKilled`), node termination, or network partitions occurring immediately after the commit hook triggers.
  - Yields unrecoverable message loss in mission-critical workflows.

### Option C: Polling Transactional Outbox Pattern
- **Mechanism**: Persist both the domain entity and an outbox event record within the *same* local PostgreSQL ACID transaction. A background poller fetches pending outbox events using row-level locks and publishes them to Kafka with delivery acknowledgments.
- **Verdict**: **Accepted**.
- **Rationale**:
  - Guarantees **At-Least-Once Delivery** with zero data loss ($RPO = 0$).
  - Decouples transactional commit latency from external Kafka broker latency.
  - Completely contained within the database engine without external infrastructure dependencies.

### Option D: Change Data Capture (CDC) via Debezium
- **Mechanism**: Tail the PostgreSQL Write-Ahead Log (WAL) via Debezium and Kafka Connect to stream outbox records directly into Kafka topics.
- **Verdict**: **Evaluated for Future Scale-Out**.
- **Rationale**:
  - CDC minimizes database polling overhead and achieves sub-10ms delivery latency.
  - However, running Kafka Connect introduces additional infrastructure, schema registries, and operational complexity. The Polling Transactional Outbox provides immediate zero-dependency operational simplicity for current workloads, with an outbox schema explicitly forward-compatible with Debezium CDC ingestion.

---

## 3. Outbox Table Schema & Partial Indexing Design

### 3.1 PostgreSQL DDL Specification

```sql
CREATE TABLE outbox_messages (
    id                UUID                     PRIMARY KEY,
    aggregate_type    VARCHAR(64)              NOT NULL,
    aggregate_id      VARCHAR(64)              NOT NULL,
    event_type        VARCHAR(64)              NOT NULL,
    payload           JSONB                    NOT NULL,
    status            VARCHAR(32)              NOT NULL DEFAULT 'PENDING',
    retry_count       INT                      NOT NULL DEFAULT 0,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at      TIMESTAMP WITH TIME ZONE
);

-- Partial index strictly covering pending records
CREATE INDEX idx_outbox_pending_poll 
    ON outbox_messages (status, created_at) 
    WHERE status = 'PENDING';
```

### 3.2 Concurrency Control

To support horizontal autoscaling without worker collisions, outbox pollers query records using pessimistic write locking:

```sql
SELECT m FROM OutboxMessage m
WHERE m.status = :status AND m.retryCount < :maxRetries
ORDER BY m.createdAt ASC
```

---

## 4. Distributed Tracing Propagation (W3C TraceContext)

To ensure end-to-end distributed observability across asynchronous Kafka boundaries, OpenTelemetry and Micrometer Tracing bridge the transactional outbox:

1. **Extraction on Ingestion**: The incoming HTTP span captures the W3C `traceparent` (Trace ID, Span ID, Trace Flags).
2. **Kafka Header Injection**: The outbox relay worker injects the active `traceparent` into Kafka `RecordHeader` attributes.
3. **Consumer Continuation**: The downstream consumer extracts the trace context via OpenTelemetry, creating a child span linked to the originating transaction.

---

## 5. Idempotent Consumer Requirements & Deduplication Strategy

Because the Transactional Outbox pattern guarantees **At-Least-Once Delivery**, network retransmissions, broker rebalances, or publisher retries will produce duplicate events. Downstream consumers **must** be strictly idempotent.

- Consumer checks incoming `eventId` from the Kafka header against a deduplication store or relational table.
- If already processed, the consumer acknowledges the offset and skips execution.

---

## 6. Resilience & Backpressure

### 6.1 Resilience4j Integration
- **Circuit Breaker**: Monitored on outbound calls. If downstream systems or brokers suffer continuous timeouts, the circuit breaker opens, preventing database connection pool starvation.
- **Exponential Backoff**: Transient database or network blips back off exponentially before incrementing `retryCount`.

### 6.2 Kafka Producer Configuration
- `acks: all` ensures data is committed to all in-sync replicas before returning ACK.
- `enable.idempotence: true` prevents producer-level duplicates within the same partition session.

---

## 7. Production SLAs & Verification

| Metric / Objective | SLA Target | Measurement Instrument |
| :--- | :--- | :--- |
| **Outbox-to-Broker Latency** | $p95 < 150\text{ ms}$, $p99 < 300\text{ ms}$ | Micrometer Timer (`business.orders.creation.duration`) |
| **Message Loss Tolerance** | $0.000\%$ ($RPO = 0$) | Prometheus Gauge (`outbox.pending.count`) |
| **Deduplication Precision** | $100\%$ duplicate rejection | Integration Tests with Testcontainers |
| **Code Coverage Threshold** | $\ge 80\%$ line coverage | JaCoCo Maven Plugin Verification |
| **Dependency Vulnerability** | 0 vulnerabilities with $\text{CVSS} \ge 7.0$ | OWASP Dependency-Check |
