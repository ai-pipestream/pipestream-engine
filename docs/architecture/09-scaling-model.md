# Scaling Model

The Pipestream Scaling Model keeps things simple: monitor key metrics, scale when thresholds are crossed. Start with conservative settings and tune based on real-world data.

## What Needs to Scale

| Component | When to Scale Up | When to Scale Down |
|-----------|------------------|-------------------|
| **Engine + Sidecar** | Kafka lag growing, CPU high | Lag cleared, CPU low |
| **Modules (CPU)** | Request latency increasing | Requests drop |
| **Modules (GPU)** | Same as CPU, but scale to zero when idle | No pending work |

## Key Metrics to Monitor

### Engine + Sidecar

| Metric | Threshold (starting point) | Action |
|--------|---------------------------|--------|
| CPU utilization | > 70% for 5 min | Add instance |
| Kafka consumer lag | > 1000 messages | Add instance |
| gRPC latency p99 | > 500ms | Add instance |

### Modules

| Metric | Threshold (starting point) | Action |
|--------|---------------------------|--------|
| gRPC request queue | > 10 pending | Add instance |
| Processing latency p99 | > 2s (CPU) / > 10s (GPU) | Add instance |
| No requests | 5 min idle | Scale down (to zero for GPU) |

## Scaling Principles

**Start simple:**
- Begin with fixed instance counts
- Add autoscaling when you see patterns
- Tune thresholds based on actual load

**Separate concerns:**
- Engine/Sidecar: Always-on, scale horizontally
- CPU Modules: Scale with load, can stay at minimum 1
- GPU Modules: Scale to zero when idle (cost savings)

**Kafka as buffer:**
- Kafka absorbs burst traffic
- Lag is expected during spikes
- Scale based on lag growth rate, not absolute lag

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     SCALING ZONES                                │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Always-On Pool (Engine + Sidecar)                      │    │
│  │  • Scale: 2-10 instances based on CPU/lag               │    │
│  │  • Never scale to zero                                  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Demand-Driven Pool (CPU Modules)                       │    │
│  │  • Scale: 1-20 instances based on request load          │    │
│  │  • Minimum 1 to avoid cold starts                       │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Scale-to-Zero Pool (GPU Modules)                       │    │
│  │  • Scale: 0-5 instances based on pending work           │    │
│  │  • Accept cold start latency for cost savings           │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation: AWS Fargate

Fargate uses CloudWatch Alarms and Application Auto Scaling.

### Engine + Sidecar Service

```json
{
  "serviceName": "pipestream-engine",
  "desiredCount": 2,
  "capacityProviderStrategy": [
    {
      "capacityProvider": "FARGATE",
      "weight": 1
    }
  ]
}
```

**Auto Scaling Policy (CPU-based):**

```json
{
  "policyName": "engine-cpu-scaling",
  "policyType": "TargetTrackingScaling",
  "targetTrackingScalingPolicyConfiguration": {
    "targetValue": 70.0,
    "predefinedMetricSpecification": {
      "predefinedMetricType": "ECSServiceAverageCPUUtilization"
    },
    "scaleInCooldown": 300,
    "scaleOutCooldown": 60
  }
}
```

**Auto Scaling Policy (Kafka lag via custom metric):**

```json
{
  "policyName": "engine-lag-scaling",
  "policyType": "TargetTrackingScaling",
  "targetTrackingScalingPolicyConfiguration": {
    "targetValue": 1000.0,
    "customizedMetricSpecification": {
      "metricName": "ConsumerLag",
      "namespace": "Pipestream/Kafka",
      "statistic": "Maximum",
      "dimensions": [
        {
          "name": "ConsumerGroup",
          "value": "pipestream-sidecar"
        }
      ]
    },
    "scaleInCooldown": 300,
    "scaleOutCooldown": 60
  }
}
```

### GPU Module (Scale to Zero)

Fargate doesn't natively support scale-to-zero, but you can approximate it:

```json
{
  "serviceName": "embedder-module",
  "desiredCount": 0,
  "capacityProviderStrategy": [
    {
      "capacityProvider": "FARGATE",
      "weight": 1
    }
  ]
}
```

Use a Lambda or Step Function to:
1. Watch for messages on embedder topic
2. Set `desiredCount = 1` when work appears
3. Set `desiredCount = 0` after 5 min idle

## Implementation: Kubernetes

Kubernetes uses HPA for CPU-based scaling and KEDA for event-driven scaling.

### Engine + Sidecar Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipestream-engine
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: engine
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
        - name: sidecar
          resources:
            requests:
              cpu: "500m"
              memory: "2Gi"
            limits:
              cpu: "1"
              memory: "4Gi"
```

**HPA (CPU-based):**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: engine-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: pipestream-engine
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

**KEDA (Kafka lag-based):**

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: engine-kafka-scaler
spec:
  scaleTargetRef:
    name: pipestream-engine
  minReplicaCount: 2
  maxReplicaCount: 10
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:9092
        consumerGroup: pipestream-sidecar
        topic: pipestream.cluster1.node-001
        lagThreshold: "1000"
```

### GPU Module (Scale to Zero with KEDA)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: embedder-scaler
spec:
  scaleTargetRef:
    name: embedder-module
  minReplicaCount: 0
  maxReplicaCount: 5
  cooldownPeriod: 300
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        query: sum(rate(grpc_server_started_total{service="embedder"}[1m]))
        threshold: "1"
```

## Comparison

| Aspect | Fargate | Kubernetes |
|--------|---------|------------|
| **Setup complexity** | Lower | Higher |
| **Scale-to-zero** | Manual (Lambda trigger) | Native (KEDA) |
| **Kafka lag scaling** | Custom metric required | KEDA built-in |
| **GPU support** | Limited | Better |
| **Cost model** | Pay per task-hour | Pay for nodes |
| **Cold start** | ~30-60s | ~10-30s |
| **Operational overhead** | AWS managed | You manage cluster |

## Recommendations

**Start here:**
1. Fixed instance counts initially
2. Monitor metrics for a week
3. Add CPU-based autoscaling
4. Add lag-based scaling if needed
5. Tune thresholds based on observed patterns

**Don't over-optimize:**
- Kafka handles bursts naturally
- A few seconds of lag is fine
- Cold starts for GPU modules are acceptable
- Scale down slowly, scale up quickly

**Cost vs Latency tradeoff:**

| Priority | Engine Min | CPU Module Min | GPU Module Min |
|----------|------------|----------------|----------------|
| **Low cost** | 1 | 1 | 0 |
| **Balanced** | 2 | 1 | 0 |
| **Low latency** | 3+ | 2+ | 1 |
