# Dead Letter Queue (DLQ) Handling

## Overview

DLQs capture messages that can't be processed successfully. They prevent poison messages from blocking the pipeline while preserving them for investigation and replay.

## DLQ Structure

One DLQ topic per node:

```
dlq.{cluster_id}.{node-uuid}
```

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

```protobuf
message DlqMessage {
  // The failed stream (with document_ref to repo)
  PipeStream stream = 1;
  
  // Error details
  string error_type = 2;           // "MODULE_UNAVAILABLE", "TIMEOUT", etc.
  string error_message = 3;
  google.protobuf.Timestamp failed_at = 4;
  
  // Retry context
  int32 retry_count = 5;
  string failed_node_id = 6;
  string original_topic = 7;
  int64 original_offset = 8;
}
```

## DLQ Configuration

```protobuf
message DlqConfig {
  bool enabled = 1;                          // default: true
  optional string topic = 2;                 // override default naming
  int32 max_retries = 3;                     // default: 3
  google.protobuf.Duration retry_backoff = 4;
}
```

Configured per node in `GraphNode.dlq_config`.

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
```

## Logical Failures (Not DLQ)

When a module returns `success=false`, it's a **logical** failure (e.g., "couldn't parse this PDF"). This is NOT a DLQ case:

```java
ProcessDataResponse response = module.processData(request);

if (!response.getSuccess()) {
    // Log error in stream history
    stream = appendErrorToHistory(stream, response.getErrorMessage());
    
    // Continue to next nodes (with original doc)
    routeToNextNodes(stream, request.getDocument());
}
```

## DLQ Replay

### V1: Manual

```bash
# Admin republishes DLQ messages
kafka-console-consumer --topic dlq.prod.{node-uuid} \
  | kafka-console-producer --topic prod.{node-uuid}
```

### V2: Admin API

```java
@POST("/admin/dlq/{nodeId}/replay")
public void replayDlq(String nodeId, ReplayRequest request) {
    String dlqTopic = "dlq." + clusterId + "." + nodeId;
    String targetTopic = graphCache.getNode(nodeId).getKafkaInputTopic();
    
    consumer.subscribe(dlqTopic);
    while (hasMore(request)) {
        DlqMessage msg = consumer.poll();
        kafkaProducer.send(targetTopic, msg.getStream());
    }
}
```

## Monitoring

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
