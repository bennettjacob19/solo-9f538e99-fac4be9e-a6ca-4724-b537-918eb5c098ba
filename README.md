# 节点排空值班台

检修 Kubernetes 节点前的排空工具：FastAPI 网页选节点，先封锁，再按轮次驱逐
Pod，只在集群确认后才提示“可下线”。

首页还提供「抓下次重启日志」：容器接连重启时，`kubectl logs --previous`
只能看到最近一次退出，早先那次崩溃的日志会被覆盖。值班台可以先选好命名空间、
Pod 和普通容器，点击后再让容器崩溃，抓取那一次退出的日志。

## 规则

| 要求 | 实现 |
| --- | --- |
| 先标记不可调度，再列出本轮 Pod | `DrainManager.start()` 先 `PATCH` 节点 `spec.unschedulable=true`，再 `GET` 该节点的 Pod；封锁失败则不会产生轮次、也不会展示任何待驱逐 Pod |
| 保留 DaemonSet 与静态 Pod | `Pod.exempt_reason`：`DaemonSet`、`静态 Pod`（mirror pod / config.source=file,http）、以及已结束（Succeeded/Failed）的 Pod 均不驱逐，也不阻挡“可下线” |
| 用 Eviction 按 UID 驱逐 | `POST /api/v1/namespaces/{ns}/pods/{name}/eviction`（`policy/v1`），`deleteOptions.preconditions.uid` 为列表时的 UID |
| 同名替代者留待续排 | 若同名 Pod 的 UID 已变化，旧 UID 的驱逐决定不迁移；新对象在执行中被标为 `留待续排`，在重新读取时进入新的一轮 |
| 两节点并行不超共用 PDB 额度 | 预算判定完全交给 API Server：它返回 429 即视为“受预算阻挡”。应用不做本地配额缓存，因此并行轮次共享同一份额度 |
| 页面标出受预算阻挡的 Pod，同轮继续 | 429 只影响该 Pod 的状态行（`受预算阻挡`），循环继续处理下一个 Pod |
| 中断保留封锁与已发生的驱逐 | 中断只阻止后续驱逐；节点保持封锁，轮次记录与已发出的驱逐都保留 |
| 继续排空重新读取集群 | `resume()` 重新 `GET` 节点与 Pod，必要时补封锁，然后 `refresh()` 对账，不会沿用旧快照 |
| 获准驱逐但未离开显示“待退出” | 驱逐成功后状态为 `待退出`，直到再次检查发现对象已消失才变为 `已离开` |
| 复核后才显示可下线 | `_verify_offline()` 每次独立重读：节点当前仍不可调度 **且** 已无普通 Pod，才置为“可下线” |
| 本地演练端 | `simulator/` 提供本地 Kubernetes API 演练端（节点/Pod/PDB/Eviction/日志子资源），网页可调预算、同名替换、令 Pod 退出、写入崩溃日志并重启容器 |
| docker compose 启动 | `docker compose up --build` |

## 抓下次重启日志

容器崩溃后自动重启时，`previous` 日志只保留最近一次退出。抓取按下面的顺序
工作（`app/capture.py`）：

1. 在首页选择命名空间、Pod 和普通容器（`spec.containers`，不含 init / 临时
   容器），可选填 0–30 秒的读取延迟，点击「开始抓取」。
2. 立即从 Kubernetes API 读取 **Pod UID、容器重启次数、上次退出的容器 ID**
   作为基线，然后在后台等待，最多 30 秒（`DRAIN_CAPTURE_TIMEOUT` 可调）。
3. 只有观察到重启次数 **恰好等于基线 + 1** 时，才读取该容器
   `previous=true` 日志的末尾 100 行。
4. 读日志前后各核对一次：Pod UID、重启次数、上次退出的容器 ID 三项都必须与
   观察到加一时的值相同，才显示日志、容器 ID 和退出原因（含退出码）。
5. 以下情况一律提示「无法确认归属」并丢弃本次日志：
   * API 显示同名 Pod 重建（UID 变化）或 Pod 已不存在；
   * 重启次数跳过目标（一次观察内增加多次）或回退；
   * 读取期间又发生一次重启（读取前后核对不一致）。

等待超时与 API 读取失败分别有独立提示。抓取在后台线程进行，等待期间首页、
排空轮次与所有排空按钮都照常可用，互不阻塞。

| 结果 | 含义 |
| --- | --- |
| 等待重启 | 已记录基线，等待重启次数加一 |
| 已观察到重启，读取日志 | 计数恰好加一，正在延迟 / 读取 / 复核 |
| 已抓到崩溃日志 | 三项核对通过，显示末尾 100 行、容器 ID、退出原因 |
| 无法确认归属 | 上述竞争条件之一发生，日志已丢弃 |
| 等待超时 | 30 秒内没有观察到计数恰好加一 |
| API 读取失败 | 读取 Pod 状态或 previous 日志失败 |

## 启动（docker compose）

```bash
docker compose up --build
```

* 值班台：<http://localhost:8000>
* 演练控制台：<http://localhost:8080>

值班台在 compose 中通过 `DRAIN_KUBE_HOST=http://simulator:8080` 直连演练端，
无需 kubeconfig。

演练控制台的「容器崩溃演练」可以填写一段文字并重启某个容器：这段文字会成为
刚退出实例的日志，重启次数加一，下一次 previous 日志读取即可取到。给该 Pod
设置「日志读取延迟」后，可以在值班台读取期间再次重启，直观看到「无法确认归属、
丢弃日志」的结果。

## 连接真实集群

程序支持三种模式，均通过环境变量配置：

```bash
# 集群内（ServiceAccount）
DRAIN_KUBE_MODE=in-cluster

# 运维机上的 kubeconfig
DRAIN_KUBE_MODE=kubeconfig DRAIN_KUBECONFIG=~/.kube/config DRAIN_KUBE_CONTEXT=prod

# 显式 API Server + Bearer Token
DRAIN_KUBE_MODE=host DRAIN_KUBE_HOST=https://10.0.0.1:6443 \
  DRAIN_KUBE_TOKEN_FILE=/var/run/secrets/token DRAIN_KUBE_CA_CERT=/etc/ca.crt
```

所需权限（ClusterRole）：

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["get", "list"]
```

抓下次重启日志还需要 `pods/log` 的 `get` 权限（读取 `previous` 日志）。

可选：

* `DRAIN_STATE_FILE=/data/rounds.json` 将轮次记录（封锁决定、已发出的
  驱逐、状态行）落盘，进程重启后仍能“继续排空”。集群仍是唯一事实来源：续排
  时一定会重新读取节点与 Pod。
* `DRAIN_CAPTURE_TIMEOUT=30` 调整等待下一次重启的秒数。

## 本地开发

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r tests/requirements-dev.txt   # pytest + httpx
python3 -m pytest
```

不装依赖直接跑演练端与值班台：

```bash
uvicorn simulator.main:app --port 8080
DRAIN_KUBE_HOST=http://127.0.0.1:8080 DRAIN_SIM_URL=http://127.0.0.1:8080 \
  uvicorn app.main:app --port 8000
```

## 页面状态含义

| 状态 | 含义 |
| --- | --- |
| 待驱逐 | 本轮尚未对其发出 Eviction |
| 待退出 | Eviction 已被接受，Pod 对象仍在（含删除中） |
| 已离开 | 重新读取后该 UID 已不在节点上 |
| 受预算阻挡 | API Server 以 429 拒绝，通常因 PDB；同轮其他 Pod 继续 |
| 留待续排 | 同名替代者或驱逐冲突，留给下一轮处理 |

“可下线”只在轮次页面显示，并且必须同时满足：节点不可调度、节点上已无普通
Pod。DaemonSet、静态 Pod 和已结束的 Pod 不计入。

## 目录

```
app/
  main.py      FastAPI 路由（表单 POST + redirect，刷新不会重复驱逐）
  drain.py     轮次编排、状态机、复核逻辑
  capture.py   抓下次重启日志：基线、后台等待、读取前后核对归属
  kube.py      Kubernetes 客户端封装（封锁、列表、Eviction、PDB、previous 日志）
  models.py    轮次与 Pod 状态模型
  store.py     可选的轮次落盘
  templates/   页面（首页、轮次、抓取详情）
simulator/
  main.py      本地 Kubernetes API 演练端 + 控制台
tests/         针对演练端的端到端测试
```

## 设计取舍

* **预算判定以 API Server 为准。** 页面展示的 PDB 数字仅供解释；是否放行由
  429 决定。这样多节点并行、多副本进程都不会超额。
* **UID 是对象身份。** 所有驱逐都带 UID 前置条件；同名重建不会被误驱逐。
* **复核是独立读。** “可下线”不依赖轮次内缓存，每次检查都重新读取节点与 Pod。
* **中断不清状态。** 中断只切换阶段；封锁和已发出的驱逐保留，续排时重新读取。
* **归属先于内容。** 抓取日志前必须确认三项身份（Pod UID、重启次数、上次退出
  容器 ID）一致；任何一项不一致都丢弃日志，宁可没有日志，也不展示可能属于另
  一次退出的内容。
* **只在恰好加一时读取。** 计数跳过或回退意味着两次轮询之间发生了多次重启，
  `previous` 日志已经指向另一次退出，因此不读取。
* **读取前后各核对一次。** `previous` 日志读取本身不是原子操作；读取期间再次
  重启会让内容换人，所以读取后必须重读身份再判断。
* **等待不阻塞排空。** 抓取放在后台线程，页面只读观察，排空按钮始终可用。
