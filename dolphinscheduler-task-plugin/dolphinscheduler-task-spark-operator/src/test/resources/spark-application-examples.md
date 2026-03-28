# Spark Operator 集成测试指南

## 前置条件

### 1. 准备 Kubernetes 集群

```bash
# 使用 kind 创建本地集群（测试用）
kind create cluster --name spark-test

# 或使用已有集群
kubectl config use-context <your-context>
```

### 2. 安装 Spark Operator

```bash
# 添加 Helm 仓库
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# 安装 Spark Operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true \
  --set image.tag=v1beta2-1.3.8-3.1.1
```

### 3. 创建测试命名空间和 RBAC

```bash
kubectl create namespace spark-jobs

kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-jobs
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-jobs
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["create", "get", "list", "watch", "delete"]
- apiGroups: ["sparkoperator.k8s.io"]
  resources: ["sparkapplications"]
  verbs: ["create", "get", "list", "watch", "delete", "patch", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-role-binding
  namespace: spark-jobs
subjects:
- kind: ServiceAccount
  name: spark
  namespace: spark-jobs
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
EOF
```

## 集成测试用例

### 测试用例 1: 基础 Scala Spark 任务

**DolphinScheduler 任务参数**:
```json
{
  "appName": "spark-pi-test",
  "applicationType": "Scala",
  "image": "gcr.io/spark-operator/spark:v3.5.0",
  "imagePullPolicy": "IfNotPresent",
  "mainApplicationFile": {
    "resourceName": "spark-examples.jar"
  },
  "mainClass": "org.apache.spark.examples.SparkPi",
  "arguments": "1000",
  "namespace": "spark-jobs",
  "serviceAccount": "spark",
  "sparkVersion": "3.5.0",
  "driverSpec": {
    "cores": 1,
    "memory": "512m"
  },
  "executorSpec": {
    "cores": 1,
    "memory": "512m"
  },
  "executorInstances": 1
}
```

**验证**:
```bash
# 查看 SparkApplication
kubectl get sparkapplications -n spark-jobs

# 查看 Pods
kubectl get pods -n spark-jobs

# 查看日志
kubectl logs -n spark-jobs <driver-pod-name>
```

**预期结果**:
- SparkApplication 状态变为 COMPLETED
- DolphinScheduler 任务状态为 SUCCESS
- 日志中包含 "Pi is roughly 3.14..."

---

### 测试用例 2: PySpark 任务

**准备 Python 脚本** (`wordcount.py`):
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

data = ["hello world", "hello spark", "spark is awesome"]
rdd = spark.sparkContext.parallelize(data)
words = rdd.flatMap(lambda x: x.split(" "))
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

for word, count in word_counts.collect():
    print(f"{word}: {count}")

spark.stop()
```

**任务参数**:
```json
{
  "appName": "pyspark-wordcount",
  "applicationType": "Python",
  "image": "gcr.io/spark-operator/spark-py:v3.5.0",
  "mainApplicationFile": {
    "resourceName": "wordcount.py"
  },
  "namespace": "spark-jobs",
  "executorInstances": 2
}
```

---

### 测试用例 3: 使用 Volcano 调度器

**安装 Volcano**:
```bash
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml
```

**任务参数**:
```json
{
  "appName": "spark-volcano-test",
  "applicationType": "Scala",
  "image": "gcr.io/spark-operator/spark:v3.5.0",
  "mainApplicationFile": {
    "resourceName": "spark-examples.jar"
  },
  "mainClass": "org.apache.spark.examples.SparkPi",
  "namespace": "spark-jobs",
  "batchScheduler": "volcano",
  "queue": "default",
  "driverSpec": {
    "cores": 1,
    "memory": "512m"
  },
  "executorSpec": {
    "cores": 1,
    "memory": "512m"
  },
  "executorInstances": 2
}
```

**验证**:
```bash
# 检查 PodGroup 是否创建
kubectl get podgroups -n spark-jobs

# 验证调度器
kubectl get pods -n spark-jobs -o jsonpath='{.items[*].spec.schedulerName}'
```

**预期结果**:
- PodGroup 被创建
- Pods 的 schedulerName 为 "volcano"
- 所有 Pods 同时启动（Gang Scheduling）

---

### 测试用例 4: 任务失败和重试

**任务参数** (故意设置错误的 mainClass):
```json
{
  "appName": "spark-failure-test",
  "applicationType": "Scala",
  "image": "gcr.io/spark-operator/spark:v3.5.0",
  "mainApplicationFile": {
    "resourceName": "spark-examples.jar"
  },
  "mainClass": "com.example.NonExistentClass",
  "namespace": "spark-jobs",
  "restartPolicy": "OnFailure"
}
```

**验证**:
```bash
# 查看 SparkApplication 状态
kubectl describe sparkapplication spark-failure-test -n spark-jobs

# 查看重启次数
kubectl get pods -n spark-jobs -l spark-role=driver
```

**预期结果**:
- SparkApplication 状态为 FAILED
- DolphinScheduler 任务状态为 FAILURE
- 如果设置了 OnFailure，会自动重试

---

### 测试用例 5: 任务取消

**步骤**:
1. 提交一个长时间运行的任务
2. 在任务运行时，在 DolphinScheduler 中点击"停止"

**任务参数**:
```json
{
  "appName": "long-running-task",
  "applicationType": "Scala",
  "image": "gcr.io/spark-operator/spark:v3.5.0",
  "mainApplicationFile": {
    "resourceName": "spark-examples.jar"
  },
  "mainClass": "org.apache.spark.examples.SparkPi",
  "arguments": "100000000",
  "namespace": "spark-jobs"
}
```

**验证**:
```bash
# 启动任务后
kubectl get sparkapplications -n spark-jobs

# 取消任务后
kubectl get sparkapplications -n spark-jobs
# 应该看不到该应用

kubectl get pods -n spark-jobs
# Pods 应该被删除
```

**预期结果**:
- SparkApplication CRD 被删除
- 所有相关 Pods 被清理
- DolphinScheduler 任务状态为 KILLED

---

### 测试用例 6: 资源限制和 NodeSelector

**任务参数**:
```json
{
  "appName": "resource-limits-test",
  "applicationType": "Scala",
  "image": "gcr.io/spark-operator/spark:v3.5.0",
  "mainApplicationFile": {
    "resourceName": "spark-examples.jar"
  },
  "mainClass": "org.apache.spark.examples.SparkPi",
  "namespace": "spark-jobs",
  "driverSpec": {
    "cores": 2,
    "coreLimit": "2000m",
    "memory": "2g",
    "nodeSelector": "{\"disktype\": \"ssd\"}"
  },
  "executorSpec": {
    "cores": 4,
    "coreLimit": "4000m",
    "memory": "4g",
    "nodeSelector": "{\"disktype\": \"ssd\"}"
  },
  "executorInstances": 2
}
```

**验证**:
```bash
# 检查资源配置
kubectl get pods -n spark-jobs -o yaml | grep -A 10 resources

# 检查 NodeSelector
kubectl get pods -n spark-jobs -o yaml | grep -A 5 nodeSelector
```

---

## 性能测试

### 并发提交测试

**目标**: 测试 100 个任务同时提交时的系统表现

**脚本**:
```bash
#!/bin/bash

for i in {1..100}; do
  kubectl apply -f - <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: perf-test-$i
  namespace: spark-jobs
spec:
  type: Scala
  mode: cluster
  image: gcr.io/spark-operator/spark:v3.5.0
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  sparkVersion: 3.5.0
  driver:
    cores: 1
    memory: 512m
  executor:
    cores: 1
    memory: 512m
    instances: 1
EOF
done
```

**监控指标**:
- K8s API Server 响应时间
- Worker 内存使用
- 任务提交成功率

---

## 故障排查

### 问题 1: SparkApplication 一直处于 SUBMITTED 状态

**可能原因**:
- 资源不足
- 镜像拉取失败
- ServiceAccount 权限不足

**排查步骤**:
```bash
# 检查 Spark Operator 日志
kubectl logs -n spark-operator <spark-operator-pod>

# 检查 SparkApplication Events
kubectl describe sparkapplication <app-name> -n spark-jobs

# 检查节点资源
kubectl top nodes
```

### 问题 2: 无法连接 K8s API

**排查步骤**:
```bash
# 验证 kubeconfig
kubectl cluster-info

# 测试 DolphinScheduler Worker 的 K8s 访问
kubectl auth can-i create sparkapplications --namespace spark-jobs
```

### 问题 3: 资源文件找不到

**排查步骤**:
```bash
# 检查资源是否下载
ls -la /tmp/dolphinscheduler/exec/*/resource/

# 检查 Driver Pod 日志
kubectl logs -n spark-jobs <driver-pod> | grep -i "file not found"
```

---

## 清理测试环境

```bash
# 删除所有 SparkApplications
kubectl delete sparkapplications --all -n spark-jobs

# 删除命名空间
kubectl delete namespace spark-jobs

# 卸载 Spark Operator
helm uninstall spark-operator -n spark-operator

# 删除 Volcano（如果安装了）
kubectl delete -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml

# 删除测试集群
kind delete cluster --name spark-test
```
