#!/bin/bash

###############################################################################
# Spark Operator Task Plugin 测试运行脚本
# 
# 用法:
#   ./run-tests.sh [unit|integration|all]
#
# 示例:
#   ./run-tests.sh unit          # 只运行单元测试
#   ./run-tests.sh integration   # 只运行集成测试
#   ./run-tests.sh all           # 运行所有测试
###############################################################################

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 获取脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# 测试类型
TEST_TYPE="${1:-all}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Spark Operator Task Plugin 测试${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# 函数：运行单元测试
run_unit_tests() {
    echo -e "${YELLOW}>>> 运行单元测试...${NC}"
    cd "$SCRIPT_DIR"
    
    mvn clean test \
        -Dtest=SparkOperatorParametersTest,SparkOperatorTaskTest,SparkOperatorTaskChannelTest,SparkOperatorTaskChannelFactoryTest \
        -DfailIfNoTests=false
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 单元测试通过${NC}"
    else
        echo -e "${RED}✗ 单元测试失败${NC}"
        exit 1
    fi
}

# 函数：检查 K8s 环境
check_k8s_env() {
    echo -e "${YELLOW}>>> 检查 Kubernetes 环境...${NC}"
    
    # 检查 kubectl
    if ! command -v kubectl &> /dev/null; then
        echo -e "${RED}✗ kubectl 未安装${NC}"
        exit 1
    fi
    
    # 检查集群连接
    if ! kubectl cluster-info &> /dev/null; then
        echo -e "${RED}✗ 无法连接到 Kubernetes 集群${NC}"
        exit 1
    fi
    
    # 检查 Spark Operator
    if ! kubectl get crd sparkapplications.sparkoperator.k8s.io &> /dev/null; then
        echo -e "${YELLOW}⚠ Spark Operator CRD 未安装${NC}"
        echo -e "${YELLOW}请先安装 Spark Operator:${NC}"
        echo ""
        echo "  helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator"
        echo "  helm install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace"
        echo ""
        exit 1
    fi
    
    echo -e "${GREEN}✓ Kubernetes 环境检查通过${NC}"
}

# 函数：设置集成测试环境
setup_integration_env() {
    echo -e "${YELLOW}>>> 设置集成测试环境...${NC}"
    
    # 创建测试命名空间
    kubectl create namespace spark-test --dry-run=client -o yaml | kubectl apply -f -
    
    # 创建 ServiceAccount 和 RBAC
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark-test
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-role
  namespace: spark-test
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
  namespace: spark-test
subjects:
- kind: ServiceAccount
  name: spark
  namespace: spark-test
roleRef:
  kind: Role
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
EOF
    
    echo -e "${GREEN}✓ 集成测试环境设置完成${NC}"
}

# 函数：运行集成测试
run_integration_tests() {
    echo -e "${YELLOW}>>> 运行集成测试...${NC}"
    
    check_k8s_env
    setup_integration_env
    
    echo -e "${YELLOW}提交测试 SparkApplication...${NC}"
    
    # 测试 1: 基础 Spark 任务
    kubectl apply -f - <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: integration-test-pi
  namespace: spark-test
spec:
  type: Scala
  mode: cluster
  image: gcr.io/spark-operator/spark:v3.5.0
  imagePullPolicy: IfNotPresent
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
  sparkVersion: 3.5.0
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark
  executor:
    cores: 1
    memory: 512m
    instances: 1
EOF
    
    # 等待任务完成
    echo -e "${YELLOW}等待任务完成...${NC}"
    timeout=300
    elapsed=0
    while [ $elapsed -lt $timeout ]; do
        state=$(kubectl get sparkapplication integration-test-pi -n spark-test -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "UNKNOWN")
        
        echo -e "  当前状态: $state (${elapsed}s)"
        
        if [ "$state" == "COMPLETED" ]; then
            echo -e "${GREEN}✓ 集成测试任务成功完成${NC}"
            break
        elif [ "$state" == "FAILED" ] || [ "$state" == "SUBMISSION_FAILED" ]; then
            echo -e "${RED}✗ 集成测试任务失败${NC}"
            kubectl describe sparkapplication integration-test-pi -n spark-test
            exit 1
        fi
        
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    if [ $elapsed -ge $timeout ]; then
        echo -e "${RED}✗ 集成测试超时${NC}"
        exit 1
    fi
}

# 函数：清理测试环境
cleanup_tests() {
    echo -e "${YELLOW}>>> 清理测试环境...${NC}"
    
    kubectl delete sparkapplications --all -n spark-test --ignore-not-found=true
    kubectl delete namespace spark-test --ignore-not-found=true
    
    echo -e "${GREEN}✓ 测试环境已清理${NC}"
}

# 主逻辑
case "$TEST_TYPE" in
    unit)
        run_unit_tests
        ;;
    integration)
        run_integration_tests
        cleanup_tests
        ;;
    all)
        run_unit_tests
        echo ""
        run_integration_tests
        cleanup_tests
        ;;
    *)
        echo -e "${RED}未知的测试类型: $TEST_TYPE${NC}"
        echo "用法: $0 [unit|integration|all]"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ 所有测试通过！${NC}"
echo -e "${GREEN}========================================${NC}"
