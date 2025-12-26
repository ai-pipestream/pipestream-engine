# Consumer Coordination (TBD)

## Problem Statement

When using Kafka transport for certain edges, we need to coordinate which engine instances consume from which topic/partition combinations.

**Variables:**
- S = Number of engine server instances
- T = Number of topics (one per node using Kafka transport)
- P = Number of partitions per topic

**Constraints:**
- Each partition can only have ONE active consumer in a consumer group
- If S > (T × P), some engines have nothing to consume
- If S < (T × P), some engines consume multiple partitions

## Why This Matters

Engines are **not** primarily Kafka consumers. They're gRPC servers handling synchronous processing. Kafka consumption is for specific edges.

However, when Kafka edges exist:
- We need efficient partition distribution
- We want to avoid engines joining consumer groups unnecessarily
- We need graceful rebalancing on scale up/down

## Options Under Consideration

### Option 1: Native Kafka Consumer Groups

Let Kafka handle it automatically.

```
Consumer Group: "pipestream-engine-{topic}"

Kafka auto-assigns:
  Partition 0 → Engine 1
  Partition 1 → Engine 2
  Partition 2 → Engine 3
  ...

If more engines than partitions:
  Extra engines get no assignment (idle for this topic)
  But they're still busy with gRPC work
```

**Pros:**
- Zero custom code
- Battle-tested rebalancing
- Engines not truly idle (gRPC work continues)

**Cons:**
- No control over which engines get partitions
- Rebalancing pauses during scale events

### Option 2: Consul-Based Lease

```
Engine startup:
  1. Try to acquire lock: consul/pipestream/topics/{topic}/partitions/{partition}
  2. If acquired → subscribe to that partition
  3. If not → skip this topic/partition
  4. Heartbeat to maintain lease
  5. On shutdown → release lock
```

**Pros:**
- Explicit control
- Consul already in stack
- Session TTL handles failures

**Cons:**
- Custom coordination logic
- Must handle lock contention
- Another failure mode

### Option 3: Database Lease Table

```sql
CREATE TABLE kafka_partition_leases (
    topic VARCHAR(255),
    partition_id INT,
    engine_id VARCHAR(255),
    leased_at TIMESTAMP,
    expires_at TIMESTAMP,
    heartbeat_at TIMESTAMP,
    PRIMARY KEY (topic, partition_id)
);
```

```java
// Claim with optimistic locking
UPDATE kafka_partition_leases 
SET engine_id = ?, leased_at = NOW(), expires_at = NOW() + INTERVAL '30 seconds'
WHERE topic = ? AND partition_id = ? 
  AND (engine_id IS NULL OR expires_at < NOW());
```

**Pros:**
- PostgreSQL already in stack
- Queryable state
- Transactional guarantees

**Cons:**
- Polling for heartbeat
- DB as coordination point (availability concern?)

### Option 4: Hybrid Approach

```
1. Use native Kafka consumer groups (automatic rebalancing)
2. Configure KEDA to scale based on consumer lag, not partition count
3. Accept that some engines may have no Kafka partitions assigned
4. Those engines are still useful for gRPC processing
```

**Pros:**
- Simple
- Leverages existing tooling
- No custom coordination

**Cons:**
- Less control
- Potential for uneven distribution during transitions

## Questions to Resolve

1. **How many topics will use Kafka transport?** If few, coordination is simpler.

2. **What's the expected S:P ratio?** If engines >> partitions, coordination matters more.

3. **Is engine "idle for Kafka" actually a problem?** Engines handle gRPC too.

4. **Multi-threaded consumers?** One engine can consume multiple partitions concurrently.

5. **Dedicated Kafka consumers?** Should some engines be designated as Kafka-only?

## Current Status

**TBD** - Need to evaluate options against actual deployment patterns.

## Available Infrastructure

| Component | Coordination Capability |
|-----------|------------------------|
| **Kafka** | Native consumer groups, rebalancing |
| **Consul** | Distributed locks, sessions, KV store |
| **PostgreSQL** | Transactional lease table |
| **KEDA** | Autoscaling based on Kafka lag |
| **Quarkus** | Vert.x Kafka client for dynamic subscriptions |
