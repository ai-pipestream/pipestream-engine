# Dead Letter Queue (DLQ) Handling

## Overview

DLQs capture messages that can't be processed successfully. They prevent poison messages from blocking the pipeline while preserving them for investigation and replay.

## DLQ Structure

One DLQ topic per node:

```
dlq.{cluster_id}.{node_id}
```

Examples:
- `dlq.prod.tika-parser`
- `dlq.prod.embedder`
- `dlq.staging.opensearch-sink`

## What Triggers DLQ

| Scenario | DLQ? | Reason |
|----------|------|--------|
| Module returns `success=false` | ❌ No | Logical failure - continue with error logged |
| Module unreachable (Consul down) | ✅ Yes | Infrastructure failure |
| gRPC timeout | ✅ Yes | After retries exhausted |
| gRPC connection refused | ✅ Yes | Module crashed/unavailable |
| Repeated transient failures | ✅ Yes | After retry threshold |
| Invalid PipeStream (malformed) | ✅ Yes | Can't process |
| Max hops exceeded | ✅ Yes | Loop prevention |

## DLQ Message Format

The full PipeStream is sent to DLQ, with doc offloaded to S3:

```protobuf
// Kafka message to DLQ topic
message DlqMessage {
  // The failed stream
  PipeStream stream = 1;
  
  // Error details
  string error_type = 2;           // "MODULE_UNAVAILABLE", "TIMEOUT", etc.
  string error_message = 3;
  google.protobuf.Timestamp failed_at = 4;
  
  // Retry context
  int32 retry_count = 5;
  string original_topic = 6;
  int64 original_offset = 7;
}
```

## DLQ Configuration

```protobuf
message DlqConfig {
  // Whether DLQ is enabled (default: true)
  bool enabled = 1;
  
  // Custom topic (default: "dlq.{cluster_id}.{node_id}")
  optional string topic = 2;
  
  // Retries before DLQ (default: 3)
  int32 max_retries = 3;
  
  // Backoff between retries
  google.protobuf.Duration retry_backoff = 4;
}
```

Configured per node in `GraphNode`:

```protobuf
message GraphNode {
  // ... other fields ...
  DlqConfig dlq_config = 16;
}
```

## Engine DLQ Logic

```java
void processWithRetry(PipeStream stream, GraphNode node) {
    DlqConfig dlqConfig = node.getDlqConfig();
    int maxRetries = dlqConfig.getMaxRetries();
    Duration backoff = dlqConfig.getRetryBackoff();
    
    int attempt = 0;
    Exception lastError = null;
    
    while (attempt < maxRetries) {
        try {
            processNode(stream);
            return;  // Success
        } catch (ModuleUnavailableException | TimeoutException e) {
            lastError = e;
            attempt++;
            
            if (attempt < maxRetries) {
                Thread.sleep(backoff.toMillis() * attempt);  // Linear backoff
            }
        }
    }
    
    // All retries exhausted - send to DLQ
    sendToDlq(stream, node, lastError, attempt);
}

void sendToDlq(PipeStream stream, GraphNode node, Exception error, int retries) {
    String dlqTopic = node.getDlqConfig().hasTopic()
        ? node.getDlqConfig().getTopic()
        : "dlq." + node.getClusterId() + "." + node.getNodeId();
    
    // Ensure doc is offloaded
    if (stream.hasDocument()) {
        String docRef = repoService.savePipeDoc(stream.getDocument());
        stream = stream.toBuilder()
            .clearDocument()
            .setDocumentRef(docRef)
            .build();
    }
    
    DlqMessage dlqMessage = DlqMessage.newBuilder()
        .setStream(stream)
        .setErrorType(error.getClass().getSimpleName())
        .setErrorMessage(error.getMessage())
        .setFailedAt(now())
        .setRetryCount(retries)
        .build();
    
    kafkaProducer.send(dlqTopic, dlqMessage);
    
    // Metrics
    dlqCounter.labels(node.getNodeId()).inc();
}
```

## Logical Failures (Not DLQ)

When a module returns `success=false`, it's a **logical** failure (e.g., "couldn't parse this PDF"). This is NOT a DLQ case:

```java
ProcessDataResponse response = module.processData(request);

if (!response.getSuccess()) {
    // Log error in stream history
    stream = stream.toBuilder()
        .setMetadata(stream.getMetadata().toBuilder()
            .addHistory(StepExecutionRecord.newBuilder()
                .setNodeId(nodeId)
                .setStatus(FAILED)
                .setErrorMessage(response.getErrorMessage())
                .build())
            .build())
        .build();
    
    // Continue to next nodes (with original doc)
    routeToNextNodes(stream, request.getDocument());
}
```

## DLQ Replay (V1 - Manual)

For V1, DLQ replay is manual:

```bash
# Admin republishes DLQ messages to original topic
kafka-console-consumer --topic dlq.prod.tika-parser \
  | kafka-console-producer --topic prod.tika-parser
```

Or via admin API:

```java
@POST("/admin/dlq/{nodeId}/replay")
public void replayDlq(String nodeId, ReplayRequest request) {
    String dlqTopic = "dlq." + clusterId + "." + nodeId;
    String targetTopic = graphCache.getNode(nodeId).getKafkaInputTopic();
    
    // Consume from DLQ, produce to target
    consumer.subscribe(dlqTopic);
    while (hasMore(request)) {
        DlqMessage msg = consumer.poll();
        kafkaProducer.send(targetTopic, msg.getStream());
    }
}
```

## DLQ Replay (V2 - Automatic)

Future: automatic replay with exponential backoff:

```yaml
# DLQ retry policy (future)
dlq_config:
  enabled: true
  max_retries: 3
  retry_backoff: 1s
  auto_replay:
    enabled: true
    initial_delay: 5m
    max_delay: 1h
    backoff_multiplier: 2
    max_replay_attempts: 5
```

## Monitoring

```java
// Prometheus metrics
Counter dlqMessages = Counter.build()
    .name("pipestream_dlq_messages_total")
    .help("Total messages sent to DLQ")
    .labelNames("node_id", "error_type")
    .register();

Gauge dlqDepth = Gauge.build()
    .name("pipestream_dlq_depth")
    .help("Current DLQ depth")
    .labelNames("node_id")
    .register();
```

**Alerts:**

```yaml
- alert: DLQDepthHigh
  expr: pipestream_dlq_depth > 100
  for: 5m
  annotations:
    summary: "DLQ depth high for {{ $labels.node_id }}"
    
- alert: DLQGrowing
  expr: rate(pipestream_dlq_messages_total[5m]) > 1
  for: 10m
  annotations:
    summary: "DLQ growing for {{ $labels.node_id }}"
```

## DLQ Investigation

```java
// Admin API to inspect DLQ
@GET("/admin/dlq/{nodeId}")
public List<DlqEntry> getDlqEntries(String nodeId, int limit) {
    String dlqTopic = "dlq." + clusterId + "." + nodeId;
    
    return kafkaAdmin.getMessages(dlqTopic, limit).stream()
        .map(msg -> DlqEntry.builder()
            .streamId(msg.getStream().getStreamId())
            .errorType(msg.getErrorType())
            .errorMessage(msg.getErrorMessage())
            .failedAt(msg.getFailedAt())
            .retryCount(msg.getRetryCount())
            .build())
        .collect(toList());
}
```
