# T11 docker 链路首跑记录（L3 首次实跑）

> 任务：roadmap §四 T11「CI/测试深化 + docker 链路首跑」。本文如实登记
> v0.1.0 遗留的 `docker-compose.yml` + `smoke.sh --full` 链路在本机
> （Intel Mac, OrbStack, docker 29.4.0 / compose v5.1.2）的首次实跑：
> 发现的问题、修复与最终数字。

## 一、首跑结论（修复后全绿）

| 项 | 结果 |
|---|---|
| `docker compose up -d --wait`（kafka+doris fe/be 健康就绪） | ✅ 磁盘清理后 ~2min 三容器 healthy（BE 健康检查升级为「进程活 + 已注册进 FE」，见 F6） |
| `./scripts/smoke.sh --full` S0–S7（依赖无关门，回归） | ✅ 25/25 |
| S8 kafka→doris 端到端（10 条 OGG → Doris 落库） | ✅ 9/9（flushed=10，`COUNT(*)`=10） |
| S9 毒数据浸泡 ≥10min（RUNNING 全程 + 行数守恒） | ✅ 见下表数字 |
| `docker compose down -v` 后 `docker ps` 无残留 | ✅（0 容器 / 0 网络） |
| conn-kafka 活性测试（testcontainers 模式） | ✅ 1 passed（8.60s 含起容器） |
| chaos kill-9 测试（testcontainers 模式） | ✅ 1 passed（60.56s，见 chaos 段） |
| 全程墙钟 | 11m20s（13:47:55→13:59:15，含 600s 浸泡；`/tmp/smoke-full16.log`） |

首跑不是一次过的：S0–S7 一路绿、S8.1–S8.7 绿，随后 S8.8/S8.9/S9.6–S9.9
六步连续两轮全红（38/44）。逐层排查出 **三个相互叠加的真根因**（F5/F9/F10），
修复后第三轮 44/44 全绿。

## 二、首跑发现的问题（全部已修）

### F1 · `bitnami/kafka:3.9` 已从 Docker Hub 消失（2025 Bitnami 归档）

首跑 `docker compose up -d --wait` 第一步即失败：

```
Error response from daemon: failed to resolve reference
"docker.io/bitnami/kafka:3.9": docker.io/bitnami/kafka:3.9: not found
```

（`bitnami/kafka:latest` 同样 not found——不是 tag 问题，是整个免费目录
被归档。）**修复**：kafka 换官方 `apache/kafka:3.9.0`（KRaft 单节点，
`KAFKA_*` env → server.properties 约定，镜像内脚本在 `/opt/kafka/bin/`）。

### F2 · Doris 镜像 tag 上游改名，`2.1.7-be-x86_64` 404

v0.1.0 compose 引用 `apache/doris:2.1.7-fe-${DORIS_ARCH:-aarch64}`：

- `2.1.7-be-x86_64` → **not found**（镜像仓库 tag 列表确认 2.1.7 只剩
  `fe-2.1.7` / `be-2.1.7` 多架构 tag）；
- 默认 `aarch64` 后缀 tag 仅个别残留在镜像加速端可见，不可依赖。

**修复**：改用多架构 `apache/doris:fe-2.1.7` / `apache/doris:be-2.1.7`，
Docker 按宿主架构自动选择——`DORIS_ARCH` 变量整个移除（x86_64 CI runner
与 aarch64 Mac 通吃），FE_SERVERS/FE_ID/BE_ADDR env 契约不变。

### F3 · smoke.sh 的容器内脚本路径

bitnami 镜像 `kafka-topics.sh` 在 PATH 上，apache 官方镜像不在——
smoke.sh S8/S9 的容器内调用统一改为 `/opt/kafka/bin/...` 绝对路径。

### F4/F9 · 宿主磁盘满（两轮复发，环境问题，非仓库缺陷）

- 第 1 轮（前任会话）：宿主盘 100% 满，OrbStack 三次在容器层写满时崩溃；
- 第 2 轮（本会话首跑 FE）：FE 直接 `exited (0)`，日志根因是 BDB JE 的
  `DiskLimitException: freeDiskLimit=1,073,741,824 ... diskFreeSpace=598,372,352`
  ——**JE 拒绝在 <1GB 可用的盘上初始化元数据**。同一根因也是第 1 轮
  BE 注册慢/失败（`available backend num is 0`）的幕后推手。
- 处置：清 `target/debug`（17G，cargo 可再生；后续测试以
  `CARGO_PROFILE_DEV_DEBUG=0` 重编，体积 ~4G）+ dangling 镜像层；清理后
  BE **5 秒内**注册且 `Alive=true`。未动用户镜像（flink/mysql/redis/
  doris-runtime 等）与 IDE 缓存；dangling 卷列表里含用户其它项目数据卷
  （flink-ai-platform_*），**严禁 `docker volume prune`**。

### F5 · console 部署计划 stream_load_url 拼错（404 元凶之一）

console `build_plan_table` 把 sink 表的 `stream_load_url` 从
`{base}/api/{db}/{table}_stream_load` 拼 **丢了一个斜杠**（引擎原样 PUT →
BE 404 → flush fatal → 管道 auto-restart 3 次后 FAILED）。事件证据
（node-state/events.log）：

```
WARN  PIPELINE_RESTART  pipeline#2  auto-restart #1 ...: stream load failed
  label=p2-r29742_0 (attempt 1, not retryable): HTTP 404: Not Found
ERROR STATEMENT_FAILED pipeline#2  ...（3 consecutive failures without
  cursor progress — judged poison data / deterministic fault）
```

**修复**：`{base}/api/{db}/{table}/_stream_load`；并在
`console/tests/pipelines_flow.rs` 部署计划断言里**钉住完整 URL**（回归即红）。
（本轮同文件一并把 camelCase 键修正为引擎 /v1 契约的 snake_case
`stream_load_url`/`auth_env`——前任发现的「must declare auth_env」问题。）

### F6 · BE 健康检查不感知 FE 注册 / smoke 建表步假绿

- compose BE healthcheck 原来只探 `/api/health`——BE 进程活着但尚未
  `ALTER SYSTEM ADD BACKEND` 时也报 healthy，`--wait` 放行后 DDL 立刻
  死在 `errCode = 2 ... available backend num is 0`。**修复**：healthcheck
  追加 `mysql -h<fe> show backends | grep <be-ip>`（注册感知）。
- `mysql -e` 打印 SQL 错误仍可 exit 0，S8.3 曾记 "ok" 但表根本没建。
  **修复**：smoke S8.3/S9.2 增加 `doris_wait_table`（SHOW TABLES 断言）。

### F7 · OrbStack 端口发布滞后 + 容器清理竞态（chaos 测试路径）

- `docker run -d` 返回后 `docker port` 可 **>10s** 查不到映射（自定义
  网络下更久）→ chaos 测试改用 **pid 派生的确定宿主端口**（20000+pid%15000，
  避开 38030/39030/38040 compose 覆盖区），零发现依赖；
- boot 中途失败时容器/子网泄漏（Deps 未构造，Drop 不会跑）→ 下轮
  `network create` 死于 `Pool overlaps`。**修复**：boot 每步失败显式
  teardown；`docker_teardown` 带 3×2s 重试（OrbStack 下容器启动未完成时
  `docker rm -f` 可静默失败）。

### F8 · chaos 的 docker run 漏 nofile ulimit → BE 永不启动

compose 里有 `ulimits.nofile=655350`，chaos 的裸 `docker run` 没有：BE
entrypoint 打印 `* hard nofile 655350 ... run 'ulimit -n 655350'` 后
**注册进 FE 但进程拒绝启动**，`/api/health` 永不应答（240s 超时）。
**修复**：两个容器都加 `--ulimit nofile=655350:655350`。另修两处时序：
`show backends` 的 **Alive 在第 10 列**（0-based 9，不是 8——曾对着
LastHeartbeat 时间戳等 true）；DDL 前先等 `Alive=true`；kafka-native 上
fresh group 加入/再平衡要数秒，chaos 两阶段窗口 4s/6s 太短（consumed=0），
放宽为 12s(kill@5s)/25s。

### F10 · Doris CSV 载体在官方 be-2.1.7 上无法承载 RFC4180（核心修复）

对 compose 栈的 `apache/doris:be-2.1.7` 实测 stream load CSV header 矩阵
（单行受控载荷 + ErrorURL 取证）：

| 载荷形状 | header 组合 | 结果 |
|---|---|---|
| 裸三列（无引号） | 无 / enclose+escape | ✅ / ✅ |
| 含逗号引号字段 `"B,1"` | 仅 `enclose:"` | ✅ |
| 含逗号引号字段 | `enclose:"`+`escape:"` | ❌ 整行坍缩成 1 列 |
| 字段内换行 | 仅 `enclose:"` | ✅ |
| RFC4180 双写引号 `""` | 仅 enclose / enclose+`\` 转义 | ❌ 均解析失败 |
| 反斜杠转义 `\"` | `enclose:"`+`escape:\` | ✅ |

即 **be-2.1.7 的 CSV 切分器没有 RFC4180 模式**（Arrow csv writer 的默认
输出恰是 RFC4180 双写），任一 enclose/escape 组合都无法同时承载
「逗号+双写引号+换行」。**修复**：`tailrace-conn-doris` 载体从 CSV 换成
**JSON lines**（`format: json` + `read_json_by_line: true`）：serde_json
转义对任意 UTF-8 全覆盖，字段值不可能破坏帧。单测
`doris_json_carrier_poison_roundtrip` 在 MockDoris 上钉住毒值逐字节往返。
S9.9 的 Doris 端字节级验证（INSTR(note, CHAR(10))=6）确认真实换行落库。

## 三、S9 毒数据浸泡（≥10 分钟持续消费 + 行数守恒）

场景：`smoke_poison_ogg` topic 持续生产含
**字段内换行 / 双引号 / 逗号 / emoji（🎉🚀）/ 中文** 的 OGG 信封
（信封本身单行 JSON——换行在字段值里，经 decode→transform→Doris JSON
stream load 全链）；管道全程 RUNNING 且行数守恒
（produced == consumed == flushed == doris COUNT）。

| 指标 | 数字 |
|---|---|
| 浸泡时长 | 600s（默认 10min，`SMOKE_FULL_SOAK_SECS`） |
| 生产节奏 | 每 15s 一批 50 行 |
| 总行数（produced） | 1700 |
| consumed == flushed == produced | 1700 == 1700 == 1700 ✅（S9.6/S9.7） |
| Doris COUNT(*) | 1700 ✅（S9.8） |
| 毒字段值完整性（P-000001 的 note 含 line2/🎉/中文毒数据） | ✅（S9.9：换行+引号+emoji+CJK 全往返） |
| 管道状态 | RUNNING（每批次轮询断言，S9.5） |

容器内存（浸泡后 `docker stats` 快照，GitHub runner 预算依据）：

```
mem tailrace-kafka 371.7MiB / 23.54GiB (1.54%)
mem tailrace-doris-fe 867.4MiB / 23.54GiB (3.60%)
mem tailrace-doris-be 1.082GiB / 23.54GiB (4.60%)
```

## 四、testcontainers 接入（chaos + conn-kafka 活性）

- 依赖：`testcontainers 0.27.3`（feature=blocking）+
  `testcontainers-modules 0.15`（kafka 模块，默认 apache/kafka-native
  3.8.0——无 bin 脚本，topic 创建走 broker 自动建）。dev-only，
  root Cargo.toml 仅两行 workspace 依赖声明；`http_wait`/`ring` 特性
  刻意不启用（避免 rustls/aws-lc-sys C 工具链负担），就绪探针用
  既有 reqwest 轮询。
- `engine/bin/tailrace-node/tests/chaos.rs`：`TAILRACE_TESTCONTAINERS=1`
  时自起 Kafka + Doris fe/be（与 compose 相同 multi-arch tag + nofile
  ulimit + 静态 IP 子网 172.29.0.0/24，pid 派生宿主端口）→ 等 FE/BE 就绪
  **且 BE Alive=true** → 建库建表 → 播种 5 条 payload=chaos-e2e →
  kill -9 注入（12s 窗口第 5s）→ 重启（25s）→ **行数对账进测试内完成**
  （UNIQUE KEY 幂等，COUNT==5）；默认模式保留原 live-gate 语义。阶段
  日志落 `node-phase{1,2}.log` 便于尸检。
- `tailrace-conn-kafka` 活性测试：同 env 下自起 ephemeral kafka，
  **用本 crate 自己的 sink 播种 3 条、source 消回**（往返断言 + 游标
  推进）；默认 live-gate 不变。
- CI：`ci.yml` 新增 `chaos` job（`TAILRACE_TESTCONTAINERS=1` 跑上述两
  测试 + runner `sysctl vm.max_map_count=2000000`——Doris BE 前置）；
  `smoke` job 升级 `SMOKE_FULL_SOAK_SECS=180 ./scripts/smoke.sh --full`
  （浸泡缩短以入 45min job 预算；≥10min 口径由本地 OrbStack 默认 600s
  承担）。

## 五、chaos 实跑数字（OrbStack, testcontainers 模式）

| 指标 | 数字 |
|---|---|
| `cargo test -p tailrace-node --test chaos`（testcontainers 模式） | **1 passed / 0 failed，60.56s**（含起 kafka+fe+be、DDL、播种、kill-9、重启、对账） |
| kill-9 注入点 | phase1 第 5s（12s 窗口中途） |
| 重启对账 | `COUNT(*) WHERE payload='chaos-e2e'` == 5 ✅（UNIQUE KEY 幂等，不丢不重） |
| conn-kafka 活性（同模式） | **1 passed / 0 failed，8.60s**（ephemeral kafka 起停 + sink 3 条播种 + source 消回 + 游标推进断言） |
| 现场残留 | `docker ps -a` / network 均 0 ✅ |

调试过程（8 轮）：F7（端口发布滞后→确定端口；泄漏→显式 teardown+重试）、
F8（nofile ulimit、Alive 列位、DDL 抢跑、组加入窗口）逐项收敛，最终绿。

## 六、复跑指引

```bash
cd ~/GLMProjects/tailrace && ./scripts/build.sh
export TAILRACE_FE_HTTP_PORT=38030 TAILRACE_FE_MYSQL_PORT=39030 TAILRACE_BE_HTTP_PORT=38040
# （本机 doris-2.1 等既有容器占用 8030/8040/9030 时用上面的端口覆盖）
docker compose up -d --wait
SMOKE_FULL_SOAK_SECS=600 ./scripts/smoke.sh --full   # ~11-15 min（含 10min 浸泡）
docker compose down -v && docker ps                   # 无残留
# testcontainers 模式（chaos / 活性）：
TAILRACE_TESTCONTAINERS=1 cargo test -p tailrace-node --test chaos -- --nocapture
TAILRACE_TESTCONTAINERS=1 cargo test -p tailrace-conn-kafka kafka_source_live -- --nocapture
```
