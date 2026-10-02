# DolphinScheduler API 与 UI 分离设计

## 目标

将当前捆绑在 `dolphinscheduler-api` 镜像中的 Web UI 拆成独立的前端镜像和 Kubernetes 工作负载，同时保持 API 调用方的现有外部端口不变。

## 已核实的现状

- `dolphinscheduler-dist/src/main/assembly/dolphinscheduler-bin.xml` 将 `dolphinscheduler-ui/dist` 打包到发布包的 `ui/` 目录。
- `dolphinscheduler-dist/src/main/docker/api-server.dockerfile` 会完整解包该发布包，因此 API 镜像当前包含 UI。
- Helm Chart 目前只有 API Deployment/Service，没有 UI Deployment/Service。
- 前端生产环境默认使用同源路径 `/dolphinscheduler` 请求 API，适合由 UI 网关反向代理到 API Service。
- 当前 API 外部端口为 `31362`，Python API 外部端口为 `31363`。
- 当前 Jenkins 使用 KubeSphere Pipeline CRD，不是仓库内 Jenkinsfile；现有 `dolphinscheduler-api` Pipeline 从 `codex/xxt-3.4.2-home` 拉取代码并构建 API 镜像。

## 目标拓扑

```text
Browser
  │
  │ http://100.82.226.63:31364/dolphinscheduler/ui/
  ▼
dolphinscheduler-ui Deployment
  └─ Nginx
      ├─ /dolphinscheduler/ui/*  → 静态前端资源
      └─ /dolphinscheduler/*     → dolphinscheduler-api:12345

dolphinscheduler-api Deployment
  └─ API only, no UI files
      ├─ external NodePort 31362
      └─ Python API NodePort 31363
```

## 设计

### API 镜像

保留现有统一发布包和 API 启动方式，但在 API Dockerfile 解包时排除 `ui/` 内容，并在构建阶段检查 `api-server/bin/start.sh` 存在且可执行。这样不改变 API、数据库、ZooKeeper、Master、Worker 或 Alert 的运行契约。

### UI 镜像

新增基于 Nginx 的 UI 镜像：

- 复制 `dolphinscheduler-ui/dist` 到 `/usr/share/nginx/html/dolphinscheduler/ui/`。
- 保留前端构建的 `/dolphinscheduler/ui/` base path。
- 将 `/dolphinscheduler/` 反向代理到 Kubernetes 内部 API Service。
- 为前端路由提供 SPA fallback。
- UI 使用独立 Deployment 和 NodePort `31364`。

### Helm

新增 `ui` 配置段、UI Deployment、UI Service 和 Nginx 配置。API Service 继续使用现有端口和选择器；UI 只依赖 API Service，不挂载 API 的文件系统或日志卷。

### Jenkins

- 保留 API Pipeline 的仓库、分支、Harbor 和凭据配置。
- API Pipeline 构建 API-only 镜像。
- 新增 UI Pipeline，使用同一仓库和分支，执行 pnpm 前端构建并构建/推送 UI 镜像。
- API 和 UI 使用同一 commit 的不可变镜像标签；移动标签只作为人工运维便利，不作为部署唯一依据。

## 发布顺序与回滚

1. 构建并推送 UI 镜像。
2. 部署 UI，验证页面 HTTP 200、静态资源和 API 反代。
3. 构建并推送 API-only 镜像。
4. 滚动更新 API，验证 API 健康检查和 UI 登录页。
5. 如 API-only 镜像失败，回滚 API Deployment 到旧镜像；UI 保持可回滚的独立 ReplicaSet。

## 验收标准

- UI Deployment 和 API Deployment 分别存在，且 Pod 均 Ready。
- `http://100.82.226.63:31364/dolphinscheduler/ui/` 返回 200。
- UI 静态资源可以加载，浏览器请求 `/dolphinscheduler/` 能通过 UI Nginx 到达 API。
- `http://100.82.226.63:31362/dolphinscheduler/actuator/health/liveness` 返回 200。
- API 镜像内不存在 UI 静态文件，且 API 启动脚本存在。
- Master、Worker、Alert、数据库、ZooKeeper 不发生非必要变更。
- Jenkins 两条流水线均能从目标分支构建并推送对应 Harbor 镜像。

## 非目标

- 不迁移 API 的现有 NodePort。
- 不改变 DolphinScheduler API 路由、认证、数据库或调度语义。
- 不修改 Master、Worker、Alert 的镜像和部署方式。
- 不引入 Ingress、TLS 或新的域名；当前家庭网络通过 NodePort 和 Tailscale 访问。
