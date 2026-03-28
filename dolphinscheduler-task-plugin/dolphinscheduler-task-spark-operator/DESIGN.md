# DolphinScheduler Spark Operator 任务类型技术方案

**文档版本**: v1.0  
**作者**: [Your Name]  
**日期**: 2026-03-27  
**状态**: 待评审

---

## 1. 背景与动机

### 1.1 现状分析

DolphinScheduler 当前支持的 Spark 任务类型（`SPARK`）采用 `spark-submit` 命令行方式提交任务，存在以下问题：

| 问题类型 | 具体表现 | 影响 |
|---------|---------|------|
| **生命周期管理** | 需要外部实现重试、清理等逻辑 | 运维成本高 |
| **声明式不足** | 基于命令式提交，不符合 K8s 理念 | 不支持 GitOps |
| **监控困难** | 缺少 K8s 原生监控能力 | 可观测性差 |
| **配置复杂** | Volcano 等调度器需要 Pod Template | 配置门槛高 |
| **状态追踪** | 依赖外部日志和 YARN/K8s API | 实时性差 |

### 1.2 业务需求

随着公司 Kubernetes 基础设施的完善和 Spark Operator 的引入，业务团队提出以下需求：

1. **声明式任务管理**：希望 Spark 任务可以像其他 K8s 资源一样通过 YAML 管理
2. **批调度支持**：需要与 Volcano 调度器深度集成，支持 Gang Scheduling
3. **自动化运维**：需要自动重试、自动清理等能力
4. **统一监控**：希望通过 Prometheus + K8s Events 统一监控
5. **多租户隔离**：需要基于 Namespace 和 RBAC 的细粒度权限控制

### 1.3 目标

开发一个新的 Spark Operator 任务类型（`SPARK_OPERATOR`），与现有 `SPARK` 任务类型共存，提供以下能力：

- ✅ 通过 Kubernetes CRD (SparkApplication) 提交 Spark 任务
- ✅ 支持 Volcano/YuniKorn 批调度器一键配置
- ✅ 自动化生命周期管理（重试、清理、监控）
- ✅ 完整的 DolphinScheduler 工作流集成
- ✅ 向后兼容，不影响现有 `SPARK` 任务

---

## 2. 技术调研

### 2.1 Spark Operator 架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                              │
│  ┌──────────────────┐                                       │
│  │ Spark Operator   │                                       │
│  │   Controller     │                                       │
│  └────────┬─────────┘                                       │
│           │ Watch                                            │
│           ▼                                                  │
│  ┌──────────────────┐      Create Pods                      │
│  │ SparkApplication │ ───────────────────┐                 │
│  │      CRD         │                     │                 │
│  └──────────────────┘                     ▼                 │
│                              ┌─────────────────────────┐    │
│                              │  Driver Pod             │    │
│                              │  + Executor Pods        │    │
│                              └─────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

**核心概念**：
- **SparkApplication CRD**：定义 Spark 应用的完整配置
- **Spark Operator**：监听 CRD 变化，自动创建/管理 Pod
- **生命周期管理**：自动处理重启、失败、清理等

### 2.2 技术选型

| 技术组件 | 选型 | 理由 |
|---------|------|------|
| **Kubernetes Client** | Fabric8 Kubernetes Client | DolphinScheduler 已依赖，API 丰富 |
| **CRD 版本** | `v1beta2` | 当前 Spark Operator 主流版本 |
| **任务基类** | `AbstractRemoteTask` | 支持远程任务追踪 |
| **参数序列化** | Jackson (JSON) | 与现有任务类型保持一致 |

### 2.3 对比分析

#### **方案 A：扩展现有 SPARK 任务**
- ❌ 向后兼容风险高
- ❌ 代码逻辑复杂（需要同时支持两种模式）
- ✅ 用户无需学习新任务类型

#### **方案 B：新建 SPARK_OPERATOR 任务类型（推荐）**
- ✅ 清晰的职责分离
- ✅ 独立演进，互不影响
- ✅ 用户可根据场景选择
- ⚠️ 需要前端支持新任务类型

**结论**：采用方案 B，创建独立的任务类型。

---

## 3. 架构设计

### 3.1 系统架构

```
┌──────────────────────────────────────────────────────────────────┐
│                    DolphinScheduler                               │
│                                                                   │
│  ┌──────────────┐                                                │
│  │   UI         │ ─── 创建任务 ──┐                               │
│  └──────────────┘                 │                               │
│                                   ▼                               │
│  ┌──────────────┐         ┌──────────────┐                       │
│  │   API Server │ ───────▶│   Master     │                       │
│  └──────────────┘         └──────┬───────┘                       │
│                                   │ 调度                           │
│                                   ▼                               │
│                           ┌──────────────┐                       │
│                           │   Worker     │                       │
│                           │              │                       │
│                           │ ┌──────────────────────────┐        │
│                           │ │ SparkOperatorTask        │        │
│                           │ │  - submitApplication()   │        │
│                           │ │  - trackStatus()         │        │
│                           │ │  - cancelApplication()   │        │
│                           │ └──────────┬───────────────┘        │
│                           └────────────┼────────────────────────┘
│                                        │ K8s Client API
│                                        ▼
│  ┌──────────────────────────────────────────────────────────┐
│  │               Kubernetes Cluster                          │
│  │                                                            │
│  │  ┌─────────────────┐         ┌──────────────────┐        │
│  │  │ Spark Operator  │ ────▶   │ SparkApplication │        │
│  │  │   Controller    │         │       CRD        │        │
│  │  └─────────────────┘         └──────────────────┘        │
│  │                                        │                  │
│  │                                        ▼                  │
│  │                              ┌──────────────────┐        │
│  │                              │  Spark Pods      │        │
│  │                              └──────────────────┘        │
│  └──────────────────────────────────────────────────────────┘
```

### 3.2 类图设计

```
┌──────────────────────────┐
│  AbstractRemoteTask      │
│  (抽象基类)               │
└────────────┬─────────────┘
             │ 继承
             ▼
┌──────────────────────────┐
│  SparkOperatorTask       │
├──────────────────────────┤
│ - parameters             │
│ - kubernetesClient       │
│ - applicationName        │
├──────────────────────────┤
│ + init()                 │
│ + submitApplication()    │
│ + trackApplicationStatus()│
│ + cancelApplication()    │
│ + getParameters()        │
└──────────────────────────┘

┌──────────────────────────┐
│  SparkOperatorParameters │
│  (参数模型)               │
├──────────────────────────┤
│ - appName                │
│ - applicationType        │
│ - image                  │
│ - mainApplicationFile    │
│ - driverSpec             │
│ - executorSpec           │
│ - batchScheduler         │
│ - ...                    │
├──────────────────────────┤
│ + checkParameters()      │
│ + getResourceFilesList() │
└──────────────────────────┘

┌──────────────────────────┐
│ TaskChannelFactory       │
│ (SPI 工厂)               │
└────────────┬─────────────┘
             │ 实现
             ▼
┌──────────────────────────┐
│SparkOperatorTaskChannelFactory│
├──────────────────────────┤
│ + getName(): "SPARK_OPERATOR"│
│ + create(): TaskChannel  │
└──────────────────────────┘
```

### 3.3 序列图

```
User      UI        Master      Worker       SparkOperatorTask    K8s API      Spark Operator
 │         │           │           │                │                │               │
 │ 创建任务 │           │           │                │                │               │
 ├────────>│           │           │                │                │               │
 │         │  保存参数  │           │                │                │               │
 │         ├──────────>│           │                │                │               │
 │         │           │  调度任务  │                │                │               │
 │         │           ├──────────>│                │                │               │
 │         │           │           │    初始化      │                │               │
 │         │           │           ├───────────────>│                │               │
 │         │           │           │                │ 创建CRD         │               │
 │         │           │           │                ├───────────────>│               │
 │         │           │           │                │                │ 触发Reconcile │
 │         │           │           │                │                ├──────────────>│
 │         │           │           │                │                │               │ 创建Pods
 │         │           │           │                │                │<──────────────┤
 │         │           │           │  追踪状态       │                │               │
 │         │           │           │<───────────────┤                │               │
 │         │           │           │                │ 查询CRD状态     │               │
 │         │           │           │                ├───────────────>│               │
 │         │           │           │                │<───────────────┤               │
 │         │           │           │  更新状态       │                │               │
 │         │           │<──────────┤                │                │               │
 │  查看状态 │           │           │                │                │               │
 │<─────────┤           │           │                │                │               │
```

---

## 4. 详细设计

### 4.1 核心接口设计

#### **4.1.1 参数定义**

```java
@Data
@EqualsAndHashCode(callSuper = true)
public class SparkOperatorParameters extends AbstractParameters {
    
    // 必填参数
    private String appName;                     // 应用名称
    private SparkApplicationType applicationType; // 应用类型
    private String image;                       // Docker 镜像
    private ResourceInfo mainApplicationFile;   // 主应用文件
    private String namespace;                   // K8s 命名空间
    
    // 可选参数
    private String mainClass;                   // 主类（Java/Scala）
    private String arguments;                   // 应用参数
    private PodSpec driverSpec;                 // Driver 配置
    private PodSpec executorSpec;               // Executor 配置
    private Integer executorInstances;          // Executor 数量
    
    // 调度器配置
    private String batchScheduler;              // volcano/yunikorn
    private String queue;                       // 队列名称
    private String priorityClassName;           // 优先级
    
    // 生命周期
    private String restartPolicy;               // Never/OnFailure/Always
    private Long ttlSecondsAfterFinished;       // 自动清理时间
    
    @Override
    public boolean checkParameters() {
        return appName != null && !appName.isEmpty()
            && applicationType != null
            && image != null && !image.isEmpty()
            && mainApplicationFile != null
            && namespace != null && !namespace.isEmpty();
    }
}
```

#### **4.1.2 任务执行流程**

```java
public class SparkOperatorTask extends AbstractRemoteTask {
    
    @Override
    public void init() {
        // 1. 解析参数
        parameters = JSONUtils.parseObject(
            taskExecutionContext.getTaskParams(), 
            SparkOperatorParameters.class);
        
        // 2. 验证参数
        if (!parameters.checkParameters()) {
            throw new TaskException("Invalid parameters");
        }
        
        // 3. 初始化 K8s 客户端
        kubernetesClient = new KubernetesClientBuilder()
            .withConfig(Config.fromKubeconfig(kubeconfig))
            .build();
        
        // 4. 生成应用名称
        applicationName = generateApplicationName();
    }
    
    @Override
    public void submitApplication() throws TaskException {
        // 1. 构建 SparkApplication CRD
        GenericKubernetesResource sparkApp = buildSparkApplication();
        
        // 2. 创建 CRD 上下文
        CustomResourceDefinitionContext crdContext = 
            new CustomResourceDefinitionContext.Builder()
                .withGroup("sparkoperator.k8s.io")
                .withVersion("v1beta2")
                .withPlural("sparkapplications")
                .build();
        
        // 3. 提交到 K8s
        kubernetesClient
            .genericKubernetesResources(crdContext)
            .inNamespace(parameters.getNamespace())
            .create(sparkApp);
        
        // 4. 记录应用 ID
        setAppIds(applicationName);
    }
    
    @Override
    public void trackApplicationStatus() throws TaskException {
        // 1. 查询 SparkApplication CRD
        GenericKubernetesResource sparkApp = 
            kubernetesClient
                .genericKubernetesResources(crdContext)
                .inNamespace(parameters.getNamespace())
                .withName(applicationName)
                .get();
        
        // 2. 提取状态
        String state = extractState(sparkApp);
        
        // 3. 映射到 DolphinScheduler 状态
        TaskResponse response = mapStateToTaskResponse(state);
        setTaskResponse(response);
    }
    
    @Override
    public void cancelApplication() throws TaskException {
        // 删除 SparkApplication CRD
        kubernetesClient
            .genericKubernetesResources(crdContext)
            .inNamespace(parameters.getNamespace())
            .withName(applicationName)
            .delete();
    }
}
```

### 4.2 状态映射

| Spark Operator 状态 | DolphinScheduler 状态 | 说明 |
|--------------------|--------------------|------|
| `SUBMITTED` | `RUNNING` | 已提交到调度器 |
| `RUNNING` | `RUNNING` | 正在运行 |
| `SUCCEEDING` | `RUNNING` | 即将成功 |
| `COMPLETED` | `SUCCESS` | 成功完成 |
| `FAILED` | `FAILURE` | 执行失败 |
| `SUBMISSION_FAILED` | `FAILURE` | 提交失败 |
| `UNKNOWN` | `RUNNING` | 未知状态（继续等待） |

### 4.3 CRD 结构设计

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: <appName>-<taskAppId>
  namespace: <namespace>
  labels:
    dolphinscheduler.task.id: <taskAppId>
    dolphinscheduler.workflow.instance.id: <workflowInstanceId>
    dolphinscheduler.task.instance.id: <taskInstanceId>
spec:
  type: Java|Scala|Python|R
  mode: cluster
  image: <image>
  imagePullPolicy: <imagePullPolicy>
  sparkVersion: <sparkVersion>
  mainApplicationFile: local://<path>
  mainClass: <mainClass>  # Java/Scala only
  arguments: [<arg1>, <arg2>, ...]
  
  driver:
    cores: <driverCores>
    memory: <driverMemory>
    labels: {...}
    annotations: {...}
    nodeSelector: {...}
  
  executor:
    cores: <executorCores>
    memory: <executorMemory>
    instances: <executorInstances>
    labels: {...}
    annotations: {...}
    nodeSelector: {...}
  
  # Volcano 配置
  batchScheduler: volcano
  batchSchedulerOptions:
    queue: <queue>
    priorityClassName: <priorityClassName>
  
  # 生命周期
  restartPolicy:
    type: Never|OnFailure|Always
  timeToLiveSeconds: <ttl>
```

### 4.4 资源文件处理

```
下载流程:
1. DolphinScheduler 从资源中心下载 mainApplicationFile
   ↓
2. 存储到 Worker 本地: /tmp/dolphinscheduler/exec/xxx/resource/xxx.jar
   ↓
3. SparkOperatorTask 使用 local:// 协议指向本地文件
   ↓
4. Spark Operator 从 Driver Pod 读取文件并分发
```

**关键点**：
- 使用 `local://` 协议避免重复下载
- Worker 需要配置共享存储或 InitContainer

---

## 5. 实现计划

### 5.1 开发阶段（2 周）

| 阶段 | 任务 | 时间 | 负责人 |
|------|------|------|--------|
| **Phase 1** | 核心代码实现 | 3 天 | - |
| - | 参数模型 + 常量定义 | 0.5 天 | - |
| - | SparkOperatorTask 核心逻辑 | 1.5 天 | - |
| - | 任务通道 + 工厂类 | 0.5 天 | - |
| - | 单元测试 | 0.5 天 | - |
| **Phase 2** | 集成测试 | 4 天 | - |
| - | K8s 环境搭建（含 Spark Operator） | 1 天 | - |
| - | 基础功能测试 | 1 天 | - |
| - | Volcano 集成测试 | 1 天 | - |
| - | 异常场景测试 | 1 天 | - |
| **Phase 3** | 文档 + Code Review | 2 天 | - |
| - | API 文档 | 0.5 天 | - |
| - | 用户手册 | 0.5 天 | - |
| - | Code Review 修复 | 1 天 | - |
| **Phase 4** | 前端适配 | 3 天 | 前端团队 |
| - | 新增 SPARK_OPERATOR 任务类型 | 1 天 | - |
| - | 参数表单开发 | 1.5 天 | - |
| - | 前后端联调 | 0.5 天 | - |

### 5.2 测试计划

#### **5.2.1 单元测试**

```java
// 参数验证测试
@Test
public void testParameterValidation() {
    // 缺少必填参数应失败
    // 完整参数应成功
}

// CRD 构建测试
@Test
public void testBuildSparkApplication() {
    // 验证生成的 CRD 结构正确
    // 验证 Volcano 配置正确映射
}

// 状态映射测试
@Test
public void testStateMappingSparkApplication() {
    // 验证各状态正确映射
}
```

#### **5.2.2 集成测试**

| 场景 | 测试内容 | 预期结果 |
|------|---------|---------|
| **基础提交** | 提交 Scala Spark 任务 | 成功创建 CRD，任务完成 |
| **PySpark** | 提交 Python 任务 | 正确执行 Python 代码 |
| **Volcano** | 启用 Volcano 调度器 | PodGroup 正确创建 |
| **失败重试** | 任务失败后自动重试 | 根据 restartPolicy 重试 |
| **取消任务** | 中途取消任务 | CRD 和 Pods 被删除 |
| **资源限制** | 超出资源配额 | 任务 Pending 并正确上报 |
| **权限不足** | ServiceAccount 无权限 | 任务失败并有明确错误信息 |

#### **5.2.3 性能测试**

- **并发提交**：100 个任务同时提交，Worker 无异常
- **资源占用**：Worker 内存增长 < 50MB
- **状态查询**：查询延迟 < 500ms

---

## 6. 风险与挑战

### 6.1 技术风险

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|---------|
| **Spark Operator 版本兼容性** | CRD API 变化导致不兼容 | 中 | 使用稳定的 v1beta2 API，提供版本检查 |
| **K8s 权限配置复杂** | 用户配置错误导致任务失败 | 高 | 提供详细文档和自动检查脚本 |
| **资源文件分发** | local:// 路径在 K8s 中无法访问 | 中 | 使用 InitContainer 或共享存储 |
| **大规模任务并发** | K8s API 限流 | 低 | 实现请求重试和限流 |

### 6.2 运维风险

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| **Spark Operator 故障** | 所有 SPARK_OPERATOR 任务失败 | 提供降级方案（回退到 SPARK 任务） |
| **K8s 集群不稳定** | 任务调度失败 | 增加超时和重试机制 |
| **监控缺失** | 无法及时发现问题 | 集成 Prometheus + AlertManager |

### 6.3 兼容性风险

- **向后兼容**：新任务类型不影响现有 `SPARK` 任务 ✅
- **升级路径**：提供从 `SPARK` 到 `SPARK_OPERATOR` 的迁移工具
- **回滚方案**：插件可独立卸载，不影响核心功能

---

## 7. 部署方案

### 7.1 前置条件

#### **7.1.1 Kubernetes 集群**
```bash
# 安装 Spark Operator
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true

# （可选）安装 Volcano
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml
```

#### **7.1.2 RBAC 配置**
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-operator-role
  namespace: default
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "get", "list", "watch", "delete", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-operator-rolebinding
  namespace: default
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-operator-role
subjects:
- kind: ServiceAccount
  name: spark
  namespace: default
```

### 7.2 插件部署

```bash
# 编译插件
mvn clean package -DskipTests -pl dolphinscheduler-task-plugin/dolphinscheduler-task-spark-operator

# 部署到 Worker
cp dolphinscheduler-task-spark-operator/target/dolphinscheduler-task-spark-operator-*.jar \
   $DOLPHINSCHEDULER_HOME/libs/

# 重启 Worker
sh $DOLPHINSCHEDULER_HOME/bin/dolphinscheduler-daemon.sh restart worker-server
```

### 7.3 验证部署

```bash
# 检查插件是否加载
grep "SPARK_OPERATOR" $DOLPHINSCHEDULER_HOME/logs/dolphinscheduler-worker.log

# 预期输出
# TaskChannelFactory loaded: SPARK_OPERATOR
```

---

## 8. 监控与告警

### 8.1 关键指标

| 指标 | 类型 | 阈值 | 说明 |
|------|------|------|------|
| `spark_operator_task_submit_total` | Counter | - | 总提交次数 |
| `spark_operator_task_success_total` | Counter | - | 成功次数 |
| `spark_operator_task_failed_total` | Counter | - | 失败次数 |
| `spark_operator_task_duration_seconds` | Histogram | P99 < 3600s | 任务执行时长 |
| `spark_operator_k8s_api_errors_total` | Counter | < 10/min | K8s API 错误数 |

### 8.2 告警规则

```yaml
groups:
- name: spark_operator_task
  rules:
  - alert: SparkOperatorTaskFailureRateHigh
    expr: rate(spark_operator_task_failed_total[5m]) > 0.1
    for: 5m
    annotations:
      summary: "Spark Operator 任务失败率过高"
  
  - alert: SparkOperatorK8sAPIError
    expr: rate(spark_operator_k8s_api_errors_total[5m]) > 10
    for: 5m
    annotations:
      summary: "K8s API 调用频繁失败"
```

---

## 9. 后续优化

### 9.1 短期（3 个月内）

1. **前端增强**
   - 参数模板功能
   - 历史任务快速复制
   - 可视化配置向导

2. **功能完善**
   - 支持 SparkR 应用
   - 支持动态资源分配
   - 支持 Spark History Server 集成

### 9.2 中期（6 个月内）

1. **性能优化**
   - K8s API 请求批量化
   - 状态查询缓存
   - 资源文件预加载

2. **多集群支持**
   - 支持多个 K8s 集群
   - 集群负载均衡
   - 跨集群资源共享

### 9.3 长期（1 年内）

1. **智能调度**
   - 基于历史数据的资源预测
   - 自动调整 executor 数量
   - 智能队列选择

2. **成本优化**
   - Spot Instance 支持
   - 资源利用率分析
   - 成本报表

---

## 10. 评审问题

### 10.1 开放性问题

1. **是否支持 Spark 3.x 之前的版本？**
   - 建议：仅支持 3.0+，老版本使用现有 `SPARK` 任务

2. **如何处理超大 JAR 文件（> 1GB）？**
   - 建议：引入外部存储（S3/OSS），使用 HTTP 协议

3. **是否需要支持 Spark Thrift Server？**
   - 建议：暂不支持，作为后续特性

### 10.2 待确认事项

- [ ] 前端开发排期
- [ ] 测试环境 K8s 集群资源
- [ ] 文档编写责任人
- [ ] 上线时间窗口

---

## 11. 结论

### 11.1 技术可行性

✅ **高可行性**
- 基于成熟的 Spark Operator 和 Fabric8 K8s Client
- 架构清晰，实现复杂度可控
- 已有类似任务类型（K8S）可参考

### 11.2 业务价值

✅ **高价值**
- 降低 Spark 任务运维成本
- 提升 K8s 生态集成度
- 支持 Volcano 等高级调度特性
- 为未来 AI/ML 任务奠定基础

### 11.3 风险评估

⚠️ **中等风险**
- 主要风险来自 K8s 权限配置和资源文件分发
- 通过详细文档和自动化工具可有效缓解

### 11.4 推荐决策

**建议批准本方案**，理由如下：

1. 技术方案成熟，风险可控
2. 不影响现有功能，向后兼容性好
3. 业务需求明确，价值显著
4. 实现周期短（2 周），投入产出比高

---

## 附录

### A. 参考资料

- [Spark Operator Documentation](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
- [Volcano Scheduler](https://volcano.sh/)
- [Fabric8 Kubernetes Client](https://fabric8.io/kubernetes-client/)
- [DolphinScheduler Task Plugin Development Guide](https://dolphinscheduler.apache.org/)

### B. 示例配置

参见 `examples/spark-operator-task-examples.json`

### C. 性能基准

参见 `benchmarks/spark-operator-performance.md`

---

**文档结束**
