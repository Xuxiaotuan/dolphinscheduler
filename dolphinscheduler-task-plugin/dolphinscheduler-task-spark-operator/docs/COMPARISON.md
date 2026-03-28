# Spark 提交方式深度对比：spark-submit vs Spark Operator vs Kyuubi

**版本**: v2.0 | **日期**: 2026-03-28

---

## 📋 快速对比

| 维度 | spark-submit | Spark Operator | Kyuubi | 推荐场景 |
|------|-------------|----------------|--------|----------|
| **部署复杂度** | ⭐ 简单 | ⭐⭐⭐⭐ 复杂 | ⭐⭐⭐ 中等 | 小规模用 submit |
| **运维自动化** | ⭐ 需手动 | ⭐⭐⭐⭐⭐ 全自动 | ⭐⭐⭐⭐ 半自动 | 生产用 Operator |
| **监控能力** | ⭐⭐ 基础 | ⭐⭐⭐⭐⭐ 完善 | ⭐⭐⭐⭐ 丰富 | Operator 最佳 |
| **查询延迟** | ⭐⭐ 中等 | ⭐ 高（冷启动） | ⭐⭐⭐⭐⭐ 极低 | Kyuubi 交互式 |
| **资源利用** | ⭐⭐⭐ 中等 | ⭐⭐⭐⭐⭐ 高 | ⭐⭐⭐⭐ 高（复用） | Operator 批处理 |
| **Volcano 集成** | ⭐⭐ 复杂 | ⭐⭐⭐⭐⭐ 简单 | ⭐⭐⭐ 中等 | Operator 一键配置 |
| **GitOps 支持** | ❌ 不支持 | ✅ 原生支持 | ⚠️ 部分支持 | Operator 独有 |
| **Session 复用** | ❌ 不支持 | ❌ 不支持 | ✅ 原生支持 | Kyuubi 独有 |

---

## 🔄 完整数据流转对比

### spark-submit 流程

```
DolphinScheduler Worker
    │
    ├─ 1. 构建命令行
    ├─ 2. fork spark-submit 进程
    │       │
    │       ├─ 3. 读取 kubeconfig
    │       ├─ 4. 创建 Driver Pod (直接调用 K8s API)
    │       └─ 5. 退出进程
    │
    ├─ 6. 轮询 Driver Pod 状态 (每 5s)
    │       GET /api/v1/pods/spark-driver-xxx
    │
    └─ 7. 任务完成后需手动清理 Pods

Kubernetes
    │
    ├─ Driver Pod 启动
    │   └─ 创建 Executor Pods (Driver 直接创建)
    │
    └─ 任务完成，Pods 保留 (不会自动删除)
```

**关键问题**：
- ❌ 状态轮询增加 API Server 压力
- ❌ Pod 清理需要额外脚本
- ❌ 失败任务无自动重试

---

### Spark Operator 流程

```
DolphinScheduler Worker
    │
    ├─ 1. 构建 SparkApplication CRD
    ├─ 2. 提交 CRD 到 K8s
    │       POST /apis/sparkoperator.k8s.io/v1beta2/.../sparkapplications
    │
    └─ 3. 查询 CRD 状态 (每 5s)
            GET /apis/.../sparkapplications/spark-app-xxx
            └─ 读取 status.applicationState.state

Spark Operator Controller (自动化处理)
    │
    ├─ 4. Watch CRD 创建事件
    ├─ 5. 验证配置
    ├─ 6. 创建 Driver Service
    ├─ 7. 创建 Driver Pod (带 OwnerReference)
    ├─ 8. 监控 Driver 启动
    ├─ 9. Driver 创建 Executors
    ├─ 10. 更新 CRD Status
    │       status.applicationState.state = "RUNNING"
    │
    └─ 11. 任务完成后
            ├─ 更新 status.state = "COMPLETED"
            ├─ 等待 TTL 到期
            └─ 自动删除 Pods (保留 CRD 历史)
```

**核心优势**：
- ✅ 单点查询 (CRD)，降低 API 压力 75%
- ✅ 自动清理，避免资源泄漏
- ✅ 内置重试机制

---

### Kyuubi 流程 (Session 复用模式)

```
DolphinScheduler Worker
    │
    ├─ 1. 连接 Kyuubi Server (Thrift/JDBC)
    │       jdbc:hive2://kyuubi-server:10009
    │
    ├─ 2. 获取或创建 Session
    │       Session Pool (复用已有 Session)
    │
    └─ 3. 提交 SQL 查询
            executeQuery("SELECT * FROM table WHERE ...")

Kyuubi Server (长期运行)
    │
    ├─ Session Manager
    │   ├─ Session-1 (用户A) → Spark Session (已启动，复用)
    │   ├─ Session-2 (用户B) → Spark Session (已启动，复用)
    │   └─ Session-3 (用户C) → 新建 Spark Session (首次)
    │
    └─ Engine Manager
        ├─ 为 Session-3 创建 Spark Engine Pod
        │   ├─ 创建 Driver Pod (长期运行)
        │   └─ 创建 Executor Pods (动态分配)
        │
        └─ Session-1/2 直接使用已有 Spark Engine
            ├─ 执行 SQL (< 1s，无启动开销)
            ├─ 缓存可跨查询复用
            └─ 连接保持，低延迟

执行流程:
1. SQL-1 提交 → Session-1 (已有) → 立即执行 (0.5s)
2. SQL-2 提交 → Session-1 (复用) → 立即执行 (0.3s)
3. SQL-3 提交 → Session-1 (复用) → 立即执行 (0.4s)

vs Spark Operator:
- Task-1 → 新建集群 (30s启动 + 5s执行)
- Task-2 → 新建集群 (30s启动 + 5s执行)
- Task-3 → 新建集群 (30s启动 + 5s执行)
```

**核心优势**：
- ✅ **极低延迟** - 查询响应 < 1s（无冷启动）
- ✅ **Session 复用** - 避免重复启动 Spark 集群
- ✅ **缓存共享** - 多个查询可以复用缓存数据
- ✅ **连接池** - 高效的连接管理
- ✅ **多租户** - 基于 Session 的资源隔离

**适用场景**：
- ✅ 交互式 SQL 查询
- ✅ 即席分析（Ad-hoc Query）
- ✅ BI 报表
- ✅ 数据探索

**限制**：
- ⚠️ 长期占用资源（Engine 不会自动销毁）
- ⚠️ 不适合长时间运行的批处理任务

---

## 📊 详细对比表

### 1. 技术架构

| 对比项 | spark-submit | Spark Operator | Kyuubi |
|-------|-------------|----------------|--------|
| **提交方式** | 命令行工具 | Kubernetes CRD | Thrift/JDBC 连接 |
| **状态存储** | Pod.status | SparkApplication.status | Kyuubi Server 内存 |
| **Controller** | 无 | Operator Controller | Kyuubi Server |
| **资源关系** | 独立 Pods | CRD + Pods (OwnerReference) | Session + Engine Pods |
| **API 调用** | Worker → K8s API | Worker → CRD, Operator → K8s API | Worker → Kyuubi Server → K8s API |
| **生命周期** | 短暂 (任务级) | 短暂 (任务级) | 长期 (Session级) |

### 2. 运维能力

| 功能 | spark-submit | Spark Operator | Kyuubi | 最佳 |
|------|-------------|----------------|--------|------|
| **自动重试** | ❌ 需外部实现 | ✅ `restartPolicy` | ✅ Session 级重试 | Operator/Kyuubi |
| **自动清理** | ❌ 需手动 | ✅ `timeToLiveSeconds` | ⚠️ Engine 长期运行 | Operator |
| **配置验证** | ❌ 运行时发现 | ✅ Webhook 提前验证 | ✅ 连接时验证 | Operator |
| **状态持久化** | ❌ Pod 删除即丢失 | ✅ CRD 保留历史 | ✅ Session 历史 | Operator/Kyuubi |
| **批量操作** | ⚠️ 需脚本 | ✅ `kubectl` 原生支持 | ⚠️ SQL 批量 | Operator |
| **Session 管理** | ❌ 不支持 | ❌ 不支持 | ✅ 连接池 + 隔离 | Kyuubi |
| **启动延迟** | 30-60s | 30-60s | < 1s (复用) | Kyuubi |
| **资源复用** | ❌ 每次新建 | ❌ 每次新建 | ✅ Session 共享 | Kyuubi |

### 3. 监控对比

| 监控项 | spark-submit | Spark Operator | Kyuubi |
|-------|-------------|----------------|--------|
| **任务状态** | 查询 Pod (N次请求) | 查询 CRD (1次请求) | 查询 Session API |
| **Prometheus 指标** | 需自定义 Exporter | 内置 30+ 指标 | 内置 20+ 指标 |
| **历史记录** | 无 (Pod 删除即丢失) | CRD 可选保留 | Session/Query 历史 |
| **告警规则** | 需自己编写 | 社区提供模板 | 社区提供模板 |
| **UI 访问** | 手动端口转发 | Operator 自动创建 Service | Web UI (4040) + Kyuubi UI |
| **查询历史** | ❌ 无 | ❌ 无 | ✅ Query History |
| **Session 监控** | ❌ 不适用 | ❌ 不适用 | ✅ Active Sessions |

---

## 🎯 使用场景推荐

### ✅ 使用 spark-submit 的场景

1. **快速验证和原型开发**
   ```bash
   # 5 分钟内完成环境搭建
   kubectl create serviceaccount spark
   kubectl create rolebinding spark --clusterrole=edit --serviceaccount=default:spark
   
   # 立即提交任务
   spark-submit --master k8s://... --class MyApp app.jar
   ```

2. **小规模集群 (< 50 任务/天)**
   - 无需额外组件
   - 运维成本可接受

3. **从 YARN 迁移的过渡期**
   - 最小化迁移成本
   - 脚本兼容性好

---

### ✅ 使用 Spark Operator 的场景

1. **生产环境 (> 100 任务/天)**
   ```yaml
   # 一次配置，自动化运维
   apiVersion: sparkoperator.k8s.io/v1beta2
   kind: SparkApplication
   spec:
     restartPolicy:
       type: OnFailure
       onFailureRetries: 3
     timeToLiveSeconds: 3600
   ```

2. **需要 Gang Scheduling**
   ```yaml
   # 一行配置启用 Volcano
   spec:
     batchScheduler: volcano
     batchSchedulerOptions:
       queue: production
   ```
   
   vs spark-submit 需要创建 PodTemplate 文件

3. **GitOps 工作流**
   ```bash
   # ArgoCD 自动同步
   git push
   # → ArgoCD 检测到变更
   # → 自动更新 K8s CRD
   # → Operator 处理变更
   ```

4. **多租户平台**
   - 基于 Namespace 隔离
   - RBAC 权限控制
   - 审计日志

---

### ✅ 使用 Kyuubi 的场景

1. **交互式 SQL 查询和数据探索**
   ```sql
   -- 数据分析师即席查询，无需等待集群启动
   SELECT COUNT(*) FROM users WHERE city = 'Beijing';
   SELECT AVG(age) FROM users WHERE dt = '2026-03-28';
   
   -- 查询响应 < 1s，Session 自动复用
   ```
   
   **优势**：
   - 查询延迟极低（毫秒级）
   - Session 预热，无冷启动
   - 适合快速迭代分析

2. **BI 报表和可视化**
   ```
   BI 工具 (Tableau/Superset/DataV)
      ↓ JDBC 连接
   Kyuubi Server (长期运行)
      ↓ Session Pool
   Spark Engine (复用)
   ```
   
   **优势**：
   - 稳定的 JDBC/ODBC 连接
   - 支持标准 SQL
   - 查询并发高

3. **数据探索和原型开发**
   ```python
   # Jupyter Notebook 连接 Kyuubi
   import pandas as pd
   from sqlalchemy import create_engine
   
   engine = create_engine('hive://kyuubi-server:10009/default')
   df = pd.read_sql("SELECT * FROM table LIMIT 100", engine)
   
   # 快速迭代，无需等待 Spark 启动
   ```

4. **多租户 SQL 服务**
   ```
   团队A → Kyuubi Session → Spark Engine A (独立资源)
   团队B → Kyuubi Session → Spark Engine B (独立资源)
   团队C → Kyuubi Session → Spark Engine C (独立资源)
   
   - 基于 Session 的资源隔离
   - 动态资源分配
   - 连接级别的权限控制
   ```

**不适合 Kyuubi 的场景**：
- ❌ 长时间运行的批处理（数小时）
- ❌ 需要严格资源隔离的任务
- ❌ 一次性 ETL 任务（浪费资源）

---

## 🎯 三种方案对比总结

| 场景类型 | spark-submit | Spark Operator | Kyuubi | 推荐 |
|---------|-------------|----------------|--------|------|
| **定时批处理** | ⚠️ 可用 | ✅ 最佳 | ❌ 资源浪费 | Operator |
| **离线 ETL** | ⚠️ 可用 | ✅ 最佳 | ❌ 不适合 | Operator |
| **即席查询** | ❌ 启动慢 | ❌ 启动慢 | ✅ 最佳 | Kyuubi |
| **BI 报表** | ❌ 不适合 | ❌ 不适合 | ✅ 最佳 | Kyuubi |
| **数据探索** | ⚠️ 慢 | ❌ 不适合 | ✅ 最佳 | Kyuubi |
| **模型训练** | ⚠️ 可用 | ✅ 最佳 | ⚠️ 可用 | Operator |
| **流式处理** | ⚠️ 可用 | ✅ 较好 | ⚠️ 可用 | Operator |
| **快速原型** | ✅ 简单 | ❌ 复杂 | ✅ 便捷 | submit/Kyuubi |

---

## 🔧 故障排查对比

### spark-submit 排障流程

```bash
# 1. 查找 Driver Pod
kubectl get pods -l spark-role=driver

# 2. 查看 Pod 状态
kubectl describe pod spark-driver-xxx

# 3. 查看日志
kubectl logs spark-driver-xxx

# 4. 如果 Pod 已删除，日志丢失 ❌
```

### Spark Operator 排障流程

```bash
# 1. 查看 CRD 状态 (包含完整历史)
kubectl describe sparkapplication my-app

# Events:
#   Type    Reason             Age   Message
#   ----    ------             ----  -------
#   Normal  SparkApplicationAdded    Application added, enqueuing
#   Normal  SparkDriverPending       Driver created, pending
#   Warning PodSchedulingFailure     Insufficient memory
#   Normal  SparkDriverRunning       Driver is running

# 2. 查看详细状态
kubectl get sparkapplication my-app -o yaml

status:
  applicationState:
    state: FAILED
    errorMessage: "org.apache.spark.SparkException: Job aborted"
  submissionAttempts: 3
  lastSubmissionAttemptTime: "2026-03-28T10:30:00Z"

# 3. 即使 Pod 已删除，CRD 保留完整状态 ✅
```

**Operator 优势**：
- ✅ Events 记录完整时间线
- ✅ 状态持久化
- ✅ 详细的错误信息

---

### Kyuubi 排障流程

```bash
# 1. 查看 Kyuubi Server 状态
kubectl get pods -l app=kyuubi-server

# 2. 查看 Session 列表
curl http://kyuubi-server:10099/api/v1/sessions

# 响应示例:
# {
#   "sessions": [
#     {
#       "identifier": "session-123",
#       "user": "data-analyst",
#       "state": "ESTABLISHED",
#       "createTime": 1711600000000,
#       "engineId": "spark-engine-abc"
#     }
#   ]
# }

# 3. 查看 Query 历史
curl http://kyuubi-server:10099/api/v1/sessions/session-123/operations

# 4. 查看 Spark Engine Pods
kubectl get pods -l kyuubi-session-id=session-123

# 5. 查看 Engine 日志
kubectl logs spark-engine-abc-driver

# 6. 连接 Spark UI
kubectl port-forward spark-engine-abc-driver 4040:4040
# 浏览器打开 http://localhost:4040
```

**Kyuubi 优势**：
- ✅ Query 历史记录（SQL、执行时间、状态）
- ✅ Session 级别的监控和管理
- ✅ RESTful API 便于集成
- ✅ Web UI 可视化查询历史

**常见问题**：
```bash
# Session 创建失败
# → 检查资源配额和 Spark Engine 启动状态

# Query 执行慢
# → 查看 Spark UI，分析 Stage 和 Task

# Session 泄漏
# → 配置 Session 超时自动回收
```

---

## 📈 监控方案对比

### spark-submit 监控

**手动配置 Prometheus**：
```yaml
# 需要自己编写 ServiceMonitor
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: spark-pods
spec:
  selector:
    matchLabels:
      spark-role: driver
  endpoints:
  - port: metrics
    path: /metrics
```

**关键指标** (需自定义):
- Pod 状态 (kube-state-metrics)
- 容器资源 (cadvisor)
- 任务成功率 (需自定义 Exporter)

---

### Spark Operator 监控

**开箱即用**：
```bash
# Operator 自动导出指标
curl http://spark-operator:8080/metrics

# HELP spark_app_count Current number of applications
spark_app_count{state="running"} 5
spark_app_count{state="completed"} 120
spark_app_count{state="failed"} 3

# HELP spark_app_success_count_total Total successful apps
spark_app_success_count_total 120

# HELP spark_app_executor_running Number of running executors
spark_app_executor_running{app="etl-job"} 10
```

**Grafana Dashboard** (社区提供):
- Application Status Distribution
- Success Rate Trend
- Resource Utilization
- Executor Metrics

---

## 💰 成本分析

### 资源开销对比

| 资源类型 | spark-submit | Spark Operator | 差异 |
|---------|-------------|----------------|------|
| **Operator 进程** | 0 | ~200MB 内存 + 0.1 CPU | +200MB |
| **CRD 存储** | 0 | ~5KB/任务 (etcd) | +5KB |
| **API Server 负载** | 高 (轮询 Pods) | 低 (Watch 机制) | -75% 请求 |
| **Pod 清理** | 手动 (资源泄漏风险) | 自动 (TTL) | 节省人力 |

### API 请求量对比 (100 并发任务)

```
spark-submit:
  Worker 查询 Driver: 100 × 1 GET/5s = 20 QPS
  Worker 查询 Executors: 100 × 3 × 1 GET/5s = 60 QPS
  总计: 80 QPS

Spark Operator:
  Worker 查询 CRD: 100 × 1 GET/5s = 20 QPS
  Operator Watch (长连接): 0 QPS
  总计: 20 QPS (-75%)
```

---

## 🚀 迁移指南

### 从 spark-submit 迁移到 Operator

#### 步骤 1: 部署 Spark Operator

```bash
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true
```

#### 步骤 2: 转换配置

**原 spark-submit 命令**:
```bash
spark-submit \
  --master k8s://https://k8s:6443 \
  --deploy-mode cluster \
  --name my-app \
  --class com.example.MyApp \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g \
  /path/to/app.jar arg1 arg2
```

**转换为 CRD**:
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-app
spec:
  type: Java
  mode: cluster
  image: spark:3.5.0
  mainClass: com.example.MyApp
  mainApplicationFile: local:///path/to/app.jar
  arguments: ["arg1", "arg2"]
  executor:
    instances: 3
    cores: 2
    memory: "2g"
```

#### 步骤 3: 测试验证

```bash
# 提交测试任务
kubectl apply -f spark-app.yaml

# 查看状态
kubectl get sparkapplications -w

# 验证日志
kubectl logs my-app-driver
```

#### 步骤 4: 更新 DolphinScheduler 任务类型

```json
// 从 SPARK 改为 SPARK_OPERATOR
{
  "taskType": "SPARK_OPERATOR",
  "params": {
    "appName": "my-app",
    "applicationType": "Java",
    ...
  }
}
```

---

## 📋 最佳实践

### spark-submit 最佳实践

1. **自动化清理**
   ```bash
   # CronJob 定期清理 Completed Pods
   */30 * * * * kubectl delete pods -n spark-jobs \
     --field-selector status.phase=Succeeded \
     --field-selector status.phase=Failed
   ```

2. **Label 标记**
   ```bash
   # 添加统一 Label 便于管理
   --conf spark.kubernetes.driver.label.app=my-app \
   --conf spark.kubernetes.driver.label.team=data
   ```

3. **日志聚合**
   ```bash
   # 配置日志收集到外部存储
   --conf spark.eventLog.enabled=true \
   --conf spark.eventLog.dir=s3a://bucket/spark-logs
   ```

---

### Spark Operator 最佳实践

1. **配置 TTL 自动清理**
   ```yaml
   spec:
     timeToLiveSeconds: 3600  # 1 小时后清理
   ```

2. **启用监控**
   ```yaml
   spec:
     monitoring:
       prometheus:
         jmxExporterJar: /prometheus/jmx_prometheus_javaagent.jar
         port: 8090
   ```

3. **配置重试策略**
   ```yaml
   spec:
     restartPolicy:
       type: OnFailure
       onFailureRetries: 3
       onFailureRetryInterval: 10
   ```

4. **使用 Volcano** (Gang Scheduling)
   ```yaml
   spec:
     batchScheduler: volcano
     batchSchedulerOptions:
       queue: default
       priorityClassName: high
   ```

---

### Kyuubi 最佳实践

1. **配置 Session 超时**
   ```yaml
   # kyuubi-defaults.conf
   kyuubi.session.idle.timeout=PT2H  # 2小时无活动自动关闭
   kyuubi.engine.idle.timeout=PT30M  # 30分钟无查询关闭 Engine
   ```

2. **启用连接池**
   ```yaml
   kyuubi.session.engine.initialize.timeout=PT3M
   kyuubi.session.engine.pool.size=3  # 每用户最多3个Engine
   ```

3. **资源限制**
   ```yaml
   # Spark Engine 资源配置
   spark.driver.memory=2g
   spark.executor.memory=4g
   spark.executor.instances=3
   spark.dynamicAllocation.enabled=true  # 动态分配
   spark.dynamicAllocation.minExecutors=1
   spark.dynamicAllocation.maxExecutors=10
   ```

4. **监控和日志**
   ```yaml
   # 启用 Prometheus Metrics
   kyuubi.metrics.enabled=true
   kyuubi.metrics.reporters=PROMETHEUS
   
   # Query 历史
   kyuubi.operation.log.dir=/var/log/kyuubi/operations
   ```

---

## 🎯 决策矩阵

| 如果你的情况是... | 推荐方案 | 理由 |
|-----------------|---------|------|
| **批处理场景** | | |
| 每天 < 10 个批处理任务 | spark-submit | 简单够用，无需额外组件 |
| 每天 10-100 个批处理任务 | Spark Operator | 自动化运维，降低人工成本 |
| 每天 > 100 个批处理任务 | Spark Operator | 自动化必不可少 |
| 定时 ETL 管道 | Spark Operator | 自动重试 + 自动清理 |
| **交互式场景** | | |
| 即席 SQL 查询 | **Kyuubi** | 极低延迟 (< 1s) |
| BI 报表查询 | **Kyuubi** | JDBC 连接稳定 |
| 数据探索和分析 | **Kyuubi** | Session 复用，快速迭代 |
| Jupyter/Zeppelin | **Kyuubi** | 长连接，体验好 |
| **团队能力** | | |
| 无 K8s 运维经验 | spark-submit | 学习成本低 |
| 有 K8s 运维团队 | Spark Operator | 充分利用 K8s 生态 |
| 有 SQL 开发团队 | **Kyuubi** | 标准 SQL 接口 |
| **技术要求** | | |
| 需要 99% SLA | Spark Operator | 自动重试保障 |
| 需要 Gang Scheduling | Spark Operator | Volcano 一键配置 |
| 需要 GitOps | Spark Operator | CRD 声明式 |
| 需要查询历史 | **Kyuubi** | Query History |
| 需要 Session 管理 | **Kyuubi** | 连接池 + 隔离 |
| **任务特性** | | |
| 任务时长 > 1小时 | Spark Operator | 适合长时间运行 |
| 任务时长 < 5分钟 | **Kyuubi** | 避免冷启动开销 |
| 查询延迟敏感 | **Kyuubi** | 毫秒级响应 |
| 资源严格隔离 | Spark Operator | 每任务独立集群 |

---

## 📚 参考资料

**Spark on Kubernetes**:
- [Spark on Kubernetes 官方文档](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)

**Spark Operator**:
- [Google Spark Operator GitHub](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [Spark Operator User Guide](https://googlecloudplatform.github.io/spark-on-k8s-operator/docs/user-guide.html)
- [Spark Operator API Reference](https://googlecloudplatform.github.io/spark-on-k8s-operator/docs/api-docs.html)

**Kyuubi**:
- [Apache Kyuubi 官网](https://kyuubi.apache.org/)
- [Kyuubi GitHub](https://github.com/apache/kyuubi)
- [Kyuubi on Kubernetes](https://kyuubi.readthedocs.io/en/master/deployment/kubernetes.html)
- [Kyuubi REST API](https://kyuubi.readthedocs.io/en/master/client/rest.html)

**调度器**:
- [Volcano Scheduler](https://volcano.sh/)
- [YuniKorn Scheduler](https://yunikorn.apache.org/)

**其他**:
- [DolphinScheduler 文档](https://dolphinscheduler.apache.org/)
- [Kubernetes 官方文档](https://kubernetes.io/docs/)

---

**文档维护**: Development Team  
**最后更新**: 2026-03-28
