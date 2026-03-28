# Spark Operator 监控与故障排查完整指南

**版本**: v1.0 | **日期**: 2026-03-28

---

## 📊 监控架构

### 完整监控栈

```
┌─────────────────────────────────────────────────────────────┐
│                    DolphinScheduler                          │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐            │
│  │   UI     │────▶│  Master  │────▶│  Worker  │            │
│  │(Dashboard)│    │(Scheduler)│    │(Executor)│            │
│  └──────────┘     └──────────┘     └────┬─────┘            │
└───────────────────────────────────────────┼──────────────────┘
                                            │
                                            │ Create CRD
                                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                              │
│  ┌──────────────────────────────────────────┐              │
│  │      Spark Operator                       │              │
│  │  - Metrics Endpoint: :8080/metrics       │              │
│  │  - Health Check: :8080/health            │              │
│  └────────────┬─────────────────────────────┘              │
│               │ Manage                                       │
│               ▼                                              │
│  ┌──────────────────────────────────────────┐              │
│  │   SparkApplication CRD                    │              │
│  │  status:                                  │              │
│  │    applicationState: RUNNING             │              │
│  │    driverInfo: {...}                     │              │
│  │    executorState: {...}                  │              │
│  └──────────────────────────────────────────┘              │
│               │                                              │
│               ├─────────────────┐                           │
│               ▼                 ▼                           │
│  ┌─────────────────┐  ┌──────────────────┐                │
│  │  Driver Pod     │  │  Executor Pods   │                │
│  │  - JMX: 8090    │  │  - JMX: 8091     │                │
│  │  - UI: 4040     │  │                   │                │
│  └─────────────────┘  └──────────────────┘                │
└─────────────────────────────────────────────────────────────┘
                   │                    │
                   │                    │ Scrape Metrics
                   ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                      Prometheus                              │
│  - spark-operator ServiceMonitor                            │
│  - spark-pods ServiceMonitor (JMX Exporter)                 │
│  - kube-state-metrics (CRD & Pod 状态)                      │
│  - node-exporter (节点资源)                                  │
└────────────┬─────────────────────────────────────────────────┘
             │ Query
             ▼
┌──────────────────────────────────────────────────────────────┐
│                        Grafana                               │
│  - Spark Operator Dashboard                                 │
│  - Spark Application Dashboard                              │
│  - Resource Utilization Dashboard                           │
└──────────────────────────────────────────────────────────────┘
             │
             ├─────────────────────┐
             ▼                     ▼
┌──────────────────┐    ┌──────────────────┐
│  AlertManager    │    │  Slack/Email     │
│  (告警路由)       │───▶│  (告警通知)       │
└──────────────────┘    └──────────────────┘
```

---

## 🔍 关键监控指标

### 1. Spark Operator 指标

#### **应用级别指标**

```prometheus
# 应用状态分布
spark_app_count{namespace="spark-jobs", state="new"} 0
spark_app_count{namespace="spark-jobs", state="submitted"} 2
spark_app_count{namespace="spark-jobs", state="running"} 5
spark_app_count{namespace="spark-jobs", state="completed"} 120
spark_app_count{namespace="spark-jobs", state="failed"} 3

# 成功/失败计数
spark_app_success_count_total{namespace="spark-jobs"} 120
spark_app_failure_count_total{namespace="spark-jobs"} 3
spark_app_submit_count_total{namespace="spark-jobs"} 128

# 提交延迟 (毫秒)
spark_app_submit_latency_ms_bucket{le="1000"} 95
spark_app_submit_latency_ms_bucket{le="5000"} 120
spark_app_submit_latency_ms_bucket{le="10000"} 128

# Executor 指标
spark_app_executor_running_count{app_name="etl-job"} 10
spark_app_executor_completed_count{app_name="etl-job"} 0
spark_app_executor_failed_count{app_name="etl-job"} 2
```

#### **Operator 自身指标**

```prometheus
# Operator 健康状态
up{job="spark-operator"} 1

# Reconcile 循环性能
workqueue_depth{name="sparkapplication"} 5
workqueue_adds_total{name="sparkapplication"} 1280
workqueue_latency_seconds_bucket{name="sparkapplication", le="0.1"} 1200

# API 调用统计
rest_client_requests_total{method="GET", code="200"} 50000
rest_client_requests_total{method="POST", code="201"} 128
rest_client_request_latency_seconds{verb="GET"} 0.05
```

---

### 2. SparkApplication CRD 状态

#### **查询 CRD 状态**

```bash
# 查看所有应用
kubectl get sparkapplications -A

# 查看详细状态
kubectl get sparkapplication my-app -o yaml

status:
  applicationState:
    state: RUNNING
    errorMessage: ""
  submissionAttempts: 1
  executionAttempts: 1
  lastSubmissionAttemptTime: "2026-03-28T10:00:00Z"
  sparkApplicationId: "spark-application-1234567890"
  
  driverInfo:
    podName: my-app-driver
    webUIServiceName: my-app-ui-svc
    webUIPort: 4040
    webUIAddress: "http://my-app-ui-svc:4040"
  
  executorState:
    my-app-exec-1: RUNNING
    my-app-exec-2: RUNNING
    my-app-exec-3: FAILED
```

#### **关键状态字段**

| 字段 | 含义 | 正常值 |
|------|------|--------|
| `applicationState.state` | 应用整体状态 | RUNNING / COMPLETED |
| `submissionAttempts` | 提交尝试次数 | 1 |
| `executionAttempts` | 执行尝试次数 | 1 |
| `driverInfo.podName` | Driver Pod 名称 | 非空 |
| `executorState` | Executor 状态列表 | 大部分 RUNNING |

---

### 3. Pod 级别指标

#### **资源使用**

```prometheus
# CPU 使用率
rate(container_cpu_usage_seconds_total{
  namespace="spark-jobs",
  pod=~".*-driver|.*-exec.*"
}[5m])

# 内存使用
container_memory_usage_bytes{
  namespace="spark-jobs",
  pod=~".*-driver|.*-exec.*"
}

# 网络 I/O
rate(container_network_transmit_bytes_total{
  namespace="spark-jobs"
}[5m])
```

#### **Pod 状态**

```prometheus
# Pod 阶段
kube_pod_status_phase{
  namespace="spark-jobs",
  pod=~".*-driver|.*-exec.*",
  phase="Running"
} 1

# Pod 重启次数
kube_pod_container_status_restarts_total{
  namespace="spark-jobs"
}

# Pod 调度延迟
histogram_quantile(0.99, 
  kube_pod_start_time_seconds{namespace="spark-jobs"}
)
```

---

### 4. Spark 应用指标 (JMX)

#### **配置 JMX Exporter**

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
spec:
  monitoring:
    exposeDriverMetrics: true
    exposeExecutorMetrics: true
    prometheus:
      jmxExporterJar: "/prometheus/jmx_prometheus_javaagent.jar"
      port: 8090
  
  driver:
    javaOptions: "-javaagent:/prometheus/jmx_prometheus_javaagent.jar=8090:/etc/metrics/conf/prometheus.yaml"
  
  executor:
    javaOptions: "-javaagent:/prometheus/jmx_prometheus_javaagent.jar=8091:/etc/metrics/conf/prometheus.yaml"
```

#### **核心 Spark 指标**

```prometheus
# Stage 执行时间
metrics_stage_executorRunTime_count
metrics_stage_executorCpuTime_count

# Task 指标
metrics_executor_completedTasks_total
metrics_executor_failedTasks_total
metrics_executor_runningTasks_value

# JVM 内存
jvm_memory_used_bytes{area="heap"}
jvm_memory_max_bytes{area="heap"}

# GC 时间
jvm_gc_collection_seconds_sum{gc="PS Scavenge"}
jvm_gc_collection_seconds_sum{gc="PS MarkSweep"}

# Shuffle 指标
metrics_executor_shuffleRead_total
metrics_executor_shuffleWrite_total
```

---

## 📈 Grafana Dashboard

### Dashboard 1: Spark Operator 概览

```json
{
  "dashboard": {
    "title": "Spark Operator Overview",
    "rows": [
      {
        "title": "Applications by State",
        "panels": [
          {
            "title": "Current State Distribution",
            "targets": [{
              "expr": "sum by (state) (spark_app_count)"
            }],
            "type": "pie"
          },
          {
            "title": "Running Applications",
            "targets": [{
              "expr": "spark_app_count{state=\"running\"}"
            }],
            "type": "stat"
          }
        ]
      },
      {
        "title": "Success/Failure Metrics",
        "panels": [
          {
            "title": "Success Rate (24h)",
            "targets": [{
              "expr": "sum(rate(spark_app_success_count_total[24h])) / sum(rate(spark_app_submit_count_total[24h])) * 100"
            }],
            "type": "gauge",
            "thresholds": [
              {"value": 95, "color": "green"},
              {"value": 80, "color": "yellow"},
              {"value": 0, "color": "red"}
            ]
          },
          {
            "title": "Failure Rate Trend",
            "targets": [{
              "expr": "rate(spark_app_failure_count_total[5m])"
            }],
            "type": "graph"
          }
        ]
      },
      {
        "title": "Performance Metrics",
        "panels": [
          {
            "title": "Submit Latency P99",
            "targets": [{
              "expr": "histogram_quantile(0.99, spark_app_submit_latency_ms_bucket)"
            }],
            "type": "graph"
          }
        ]
      }
    ]
  }
}
```

### Dashboard 2: 单个应用详情

```json
{
  "dashboard": {
    "title": "Spark Application Detail",
    "templating": {
      "list": [{
        "name": "app_name",
        "type": "query",
        "query": "label_values(spark_app_count, app_name)"
      }]
    },
    "rows": [
      {
        "panels": [
          {
            "title": "Application Status",
            "targets": [{
              "expr": "spark_app_count{app_name=\"$app_name\"}"
            }]
          },
          {
            "title": "Executor Count",
            "targets": [{
              "expr": "spark_app_executor_running_count{app_name=\"$app_name\"}"
            }]
          },
          {
            "title": "CPU Usage",
            "targets": [{
              "expr": "sum(rate(container_cpu_usage_seconds_total{pod=~\"$app_name.*\"}[5m]))"
            }]
          },
          {
            "title": "Memory Usage",
            "targets": [{
              "expr": "sum(container_memory_usage_bytes{pod=~\"$app_name.*\"})"
            }]
          }
        ]
      }
    ]
  }
}
```

---

## 🚨 告警规则

### Prometheus AlertManager 配置

```yaml
groups:
- name: spark_operator_alerts
  interval: 30s
  rules:
  
  # 1. Operator 健康检查
  - alert: SparkOperatorDown
    expr: up{job="spark-operator"} == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Spark Operator 不可用"
      description: "Spark Operator 已经下线超过 5 分钟"
  
  # 2. 应用卡在 SUBMITTED 状态
  - alert: SparkApplicationStuckInSubmitted
    expr: spark_app_count{state="submitted"} > 0
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Spark 应用长时间处于 SUBMITTED 状态"
      description: "{{ $value }} 个应用卡在 SUBMITTED 状态超过 10 分钟"
  
  # 3. 高失败率
  - alert: SparkApplicationHighFailureRate
    expr: |
      (
        rate(spark_app_failure_count_total[5m]) 
        / 
        rate(spark_app_submit_count_total[5m])
      ) > 0.2
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Spark 应用失败率过高"
      description: "最近 5 分钟失败率: {{ $value | humanizePercentage }}"
  
  # 4. Executor 频繁失败
  - alert: SparkExecutorHighFailureCount
    expr: spark_app_executor_failed_count > 5
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Executor 失败次数过多"
      description: "应用 {{ $labels.app_name }} 有 {{ $value }} 个 Executor 失败"
  
  # 5. 资源使用过高
  - alert: SparkDriverHighMemoryUsage
    expr: |
      (
        container_memory_usage_bytes{pod=~".*-driver"}
        /
        container_spec_memory_limit_bytes{pod=~".*-driver"}
      ) > 0.9
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Spark Driver 内存使用过高"
      description: "Driver {{ $labels.pod }} 内存使用率: {{ $value | humanizePercentage }}"
  
  # 6. Pod 长时间 Pending
  - alert: SparkPodPendingTooLong
    expr: |
      kube_pod_status_phase{
        namespace="spark-jobs",
        phase="Pending"
      } > 0
    for: 15m
    labels:
      severity: warning
    annotations:
      summary: "Spark Pod 长时间处于 Pending 状态"
      description: "Pod {{ $labels.pod }} 已 Pending 超过 15 分钟"
  
  # 7. Reconcile 队列积压
  - alert: SparkOperatorWorkQueueBacklog
    expr: workqueue_depth{name="sparkapplication"} > 50
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Operator 工作队列积压"
      description: "当前队列深度: {{ $value }}"
```

---

## 🔧 故障排查流程

### 常见问题排查树

```
应用失败/异常
│
├─ 1. 提交失败 (SUBMISSION_FAILED)
│   ├─ 检查 Operator 日志
│   ├─ 检查 CRD Events
│   ├─ 验证 RBAC 权限
│   └─ 验证镜像是否存在
│
├─ 2. 卡在 SUBMITTED 状态
│   ├─ 检查 Operator 是否运行
│   ├─ 检查 Driver Pod 创建
│   ├─ 查看 Pod Events (调度失败?)
│   └─ 检查资源配额
│
├─ 3. Driver 启动失败 (FAILING)
│   ├─ kubectl logs <driver-pod>
│   ├─ 检查镜像拉取
│   ├─ 检查主类是否存在
│   ├─ 检查资源文件路径
│   └─ 检查依赖冲突
│
├─ 4. Executor 创建失败
│   ├─ kubectl describe pod <executor-pod>
│   ├─ 检查资源配额
│   ├─ 检查节点资源
│   └─ 检查 nodeSelector/affinity
│
├─ 5. 运行时失败 (FAILED)
│   ├─ 查看 Driver 日志
│   ├─ 查看 Executor 日志
│   ├─ 检查业务逻辑错误
│   └─ 检查数据源连接
│
└─ 6. 任务挂起/超时
    ├─ 检查网络连通性
    ├─ 检查 Shuffle 性能
    ├─ 检查 GC 情况
    └─ 检查死锁
```

---

### 故障排查命令集

#### **1. 查看应用整体状态**

```bash
# 列出所有应用
kubectl get sparkapplications -n spark-jobs

# 查看详细状态
kubectl describe sparkapplication my-app -n spark-jobs

# 查看状态字段
kubectl get sparkapplication my-app -n spark-jobs \
  -o jsonpath='{.status.applicationState.state}'

# 查看错误信息
kubectl get sparkapplication my-app -n spark-jobs \
  -o jsonpath='{.status.applicationState.errorMessage}'
```

#### **2. 查看 Events**

```bash
# CRD Events
kubectl describe sparkapplication my-app -n spark-jobs | grep -A 20 Events

# Pod Events
kubectl describe pod my-app-driver -n spark-jobs | grep -A 20 Events

# 所有 Events (按时间排序)
kubectl get events -n spark-jobs --sort-by='.lastTimestamp'
```

#### **3. 查看日志**

```bash
# Driver 日志
kubectl logs my-app-driver -n spark-jobs

# Executor 日志
kubectl logs my-app-exec-1 -n spark-jobs

# 多个 Executor 日志
kubectl logs -l spark-app-selector=my-app,spark-role=executor \
  -n spark-jobs --all-containers

# 持续查看日志
kubectl logs -f my-app-driver -n spark-jobs
```

#### **4. 查看 Pod 状态**

```bash
# 查看所有相关 Pods
kubectl get pods -l spark-app-selector=my-app -n spark-jobs

# 查看资源使用
kubectl top pod my-app-driver -n spark-jobs
kubectl top pod -l spark-app-selector=my-app -n spark-jobs

# 查看 Pod 详情
kubectl describe pod my-app-driver -n spark-jobs
```

#### **5. 调试模式**

```bash
# 进入 Driver Pod
kubectl exec -it my-app-driver -n spark-jobs -- /bin/bash

# 查看进程
ps aux | grep spark

# 查看端口监听
netstat -tulpn

# 查看文件系统
df -h
ls -la /opt/spark/work-dir
```

#### **6. 网络调试**

```bash
# 测试 Driver 到 Executor 连接
kubectl exec my-app-driver -n spark-jobs -- \
  nc -zv my-app-exec-1 7078

# 测试 DNS 解析
kubectl exec my-app-driver -n spark-jobs -- \
  nslookup my-app-driver-svc

# 访问 Spark UI
kubectl port-forward my-app-driver -n spark-jobs 4040:4040
# 浏览器打开 http://localhost:4040
```

---

### 典型故障案例

#### **案例 1: ImagePullBackOff**

**现象**:
```bash
kubectl get pod my-app-driver
NAME              READY   STATUS             RESTARTS   AGE
my-app-driver     0/1     ImagePullBackOff   0          5m
```

**排查**:
```bash
kubectl describe pod my-app-driver | grep -A 10 Events
# Events:
#   Failed to pull image "spark:3.5.0": rpc error: code = NotFound
```

**解决**:
```yaml
# 修改 CRD，使用正确的镜像
spec:
  image: gcr.io/spark-operator/spark:v3.5.0  # 完整路径
  imagePullPolicy: IfNotPresent
  imagePullSecrets:
  - name: gcr-secret  # 如果是私有仓库
```

---

#### **案例 2: OOMKilled (内存不足)**

**现象**:
```bash
kubectl get pod my-app-exec-1
NAME              READY   STATUS      RESTARTS   AGE
my-app-exec-1     0/1     OOMKilled   3          10m
```

**排查**:
```bash
# 查看资源限制
kubectl get pod my-app-exec-1 -o jsonpath='{.spec.containers[0].resources}'

# 查看实际内存使用
kubectl top pod my-app-exec-1
```

**解决**:
```yaml
# 增加 Executor 内存
spec:
  executor:
    memory: "4g"  # 从 2g 增加到 4g
    memoryOverhead: "1g"  # 增加 overhead
```

---

#### **案例 3: Insufficient Resources (资源不足)**

**现象**:
```bash
kubectl describe sparkapplication my-app | grep -A 5 Events
# Events:
#   Warning  SparkDriverPending  Driver pod is pending for more than 5 minutes
```

**排查**:
```bash
# 查看 Pod Events
kubectl describe pod my-app-driver | grep -A 10 Events
# Events:
#   Warning  FailedScheduling  0/3 nodes are available: insufficient memory

# 检查节点资源
kubectl top nodes
kubectl describe node <node-name> | grep -A 5 "Allocated resources"
```

**解决**:
```yaml
# 方案1: 减少资源请求
spec:
  driver:
    memory: "1g"  # 从 2g 减少到 1g
  executor:
    memory: "2g"  # 从 4g 减少到 2g

# 方案2: 添加节点
# 或配置 Cluster Autoscaler
```

---

#### **案例 4: Executor 频繁失败**

**现象**:
```bash
kubectl get sparkapplication my-app -o jsonpath='{.status.executorState}'
# {"exec-1": "FAILED", "exec-2": "FAILED", "exec-3": "RUNNING"}
```

**排查**:
```bash
# 查看失败 Executor 日志
kubectl logs my-app-exec-1 --previous

# 常见错误:
# - java.lang.OutOfMemoryError
# - Connection refused (网络问题)
# - Shuffle fetch failed
```

**解决**:
```yaml
# 增加 Executor 重试和资源
spec:
  executor:
    memory: "4g"
    instances: 5  # 增加实例数
  
  # 配置 Spark 参数
  sparkConf:
    "spark.task.maxFailures": "4"
    "spark.executor.heartbeatInterval": "10s"
    "spark.network.timeout": "300s"
```

---

#### **案例 5: Shuffle 性能问题**

**现象**:
- 任务执行缓慢
- Shuffle read/write 时间长

**排查**:
```bash
# 查看 Spark UI
kubectl port-forward my-app-driver 4040:4040

# 在浏览器查看:
# - Stages 页面: Shuffle Read/Write 大小
# - Executors 页面: Shuffle 指标
```

**解决**:
```yaml
spec:
  sparkConf:
    # 增加 Shuffle 分区数
    "spark.sql.shuffle.partitions": "200"
    
    # 启用压缩
    "spark.shuffle.compress": "true"
    "spark.shuffle.spill.compress": "true"
    
    # 调整缓冲区
    "spark.shuffle.file.buffer": "64k"
    "spark.reducer.maxSizeInFlight": "96m"
```

---

## 📊 性能优化建议

### 1. 资源配置优化

```yaml
# 推荐配置 (根据实际调整)
spec:
  driver:
    cores: 2
    memory: "4g"
    memoryOverhead: "1g"  # ~25% of memory
  
  executor:
    cores: 4
    memory: "8g"
    memoryOverhead: "2g"
    instances: 10
  
  # 启用动态分配
  dynamicAllocation:
    enabled: true
    initialExecutors: 3
    minExecutors: 1
    maxExecutors: 20
```

### 2. 监控采样频率

```yaml
# Prometheus 抓取配置
scrape_configs:
- job_name: 'spark-operator'
  scrape_interval: 30s  # Operator 指标
  
- job_name: 'spark-applications'
  scrape_interval: 15s  # 应用 JMX 指标
```

### 3. 日志管理

```yaml
spec:
  # 配置日志级别
  sparkConf:
    "spark.driver.extraJavaOptions": "-Dlog4j.logLevel=INFO"
    "spark.executor.extraJavaOptions": "-Dlog4j.logLevel=WARN"
  
  # 日志聚合到外部存储
  sparkConf:
    "spark.eventLog.enabled": "true"
    "spark.eventLog.dir": "s3a://bucket/spark-logs"
```

---

## 🎯 监控 Checklist

### 日常监控检查项

- [ ] Operator 健康状态 (是否运行)
- [ ] 应用成功率 (目标 > 95%)
- [ ] 平均提交延迟 (目标 < 5s)
- [ ] 队列积压 (目标 < 10)
- [ ] 资源使用率 (目标 60-80%)
- [ ] Pod 重启次数 (异常增长?)
- [ ] Executor 失败率 (目标 < 5%)

### 告警配置检查项

- [ ] Operator 下线告警
- [ ] 高失败率告警
- [ ] 资源不足告警
- [ ] 长时间 Pending 告警
- [ ] 内存使用告警

---

**维护者**: Platform Team  
**更新**: 2026-03-28
