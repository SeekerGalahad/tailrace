# tailrace

单二进制、自带 Web 控制台的轻量数据接入引擎——用 SQL 把 Kafka、文件与数据库接入 Doris / StarRocks，无 JVM、无容器、无最低 8GB 门槛。

## 状态

v0.1.0 基线（2026-10-13）。已可用：`engine/` 数据面（SQL 变换、offset 游标、kafka/file 源、Doris Stream Load 汇）与管理台（`console/` + `web/`）；mqtt/jdbc/s3 连接器为占位。

## 快速开始

前置条件：Rust ≥ 1.95（rustup；仓库 pin 了 stable 工具链）、Node.js 22、pnpm。

```bash
# 1. 构建前端并内嵌，再构建双二进制
./scripts/build.sh

# 2. 启动管理台（Web UI 在 http://127.0.0.1:8080）
./target/release/tailrace-console --data-dir ./data
#    首次启动会在 stderr 打印一次性 admin 初始密码与 bootstrap token——
#    当场抄下，之后不再展示。

# 3. 启动引擎节点并向管理台自注册
./target/release/tailrace-node serve \
  --addr 127.0.0.1:18100 \
  --control-plane http://127.0.0.1:8080 \
  --advertise 127.0.0.1:18100 \
  --bootstrap-token <首启打印的 token>
```

浏览器打开 http://127.0.0.1:8080，用 `admin` + 抄下的密码登录建管道；或跑
`./scripts/smoke.sh` 做脚本化端到端自检（25 步断言，无外部依赖）。

## 连接器矩阵

| 类型 | 源 | 汇 | 说明 |
|---|---|---|---|
| kafka | ✅ | — | 主力源，分区位点游标 |
| file | ✅ | — | 本地/挂载文件 |
| doris | — | ✅ | Stream Load |
| starrocks | — | ✅ | P2 |
| jdbc | ✅ | — | P2 |
| mqtt | ✅ | — | P2 |
| s3 | ✅ | — | P2 |

## 开发

```bash
make fmt lint test   # cargo fmt / clippy / test
make web             # pnpm install + build（web/）
./scripts/dev.sh     # console（debug）+ Vite 开发服务器（/api 代理）
```

许可证：Apache-2.0。English readme: [README.md](README.md)。
