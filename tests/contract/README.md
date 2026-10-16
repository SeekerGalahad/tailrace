# Contract tests

`tests/contract` 是真实 crate（包名 `tests-contract`，2026-10-13 落地，升级方案 §二
engine H3①②）：进程内起引擎节点（真实 axum 服务 + PipelineRegistry），以
file→file 管道覆盖 `/v1` 协议主路径与执行器主循环的确定性语义，零外部依赖
（FileSource/FileSink 纯本地），随 CI 的 `cargo test --workspace` 运行。

- `tests/v1_protocol.rs`：鉴权契约（/healthz 开放、无 token/错 token → 401）、
  stream_load_url 域白名单拒绝、plan 下推 → 列表 → stats → logs → offsets →
  pause/resume/stop 全生命周期、坏行 DLQ 契约（side 文件 + 事件带路径）。
- `tests/executor_loop.rs`：主循环确定性——游标提交点（账本只在 flush 后推进、
  pause 冻结、stop 终局收敛到 EOF）、坏行策略三分支（dlq|fail|skip）、
  dispatchId 幂等拒绝。

协议正典见 `docs/protocol/engine-v1.md`，与本目录同步演进。
