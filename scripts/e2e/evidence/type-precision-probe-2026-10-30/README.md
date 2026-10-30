# type-precision 探针（2026-10-30，R8 W5 探针先行）

对真库裸 stream load（无 console/engine），先于 `scripts/e2e/type_precision.sh`
的任何断言（R7 工单③工序）。**双版本逐行一致**：compose 2.1.7
（fe/be `apache/doris:*-2.1.7`，39030/38040）与在役 3.1.4（容器 `doris-2.1`/`doris-be`
即 `*-3.1.4`，9030/8040）。文件：

- `probe.sh` + `probe-compose-2.1.7.out` / `probe-standing-3.1.4.out`（A decimal 守恒 / B sequence_col / C 时间类型）
- `probe-t-separator.sh` + `probe-t-separator.out`（引擎 RFC3339 T 分隔形态）

复跑（mysql 客户端在容器内跑，端口恒 9030；BE 是宿主侧 curl）：

```bash
bash probe.sh standing-3.1.4 docker exec -i doris-2.1 mysql -- 8040
bash probe.sh compose-2.1.7 docker compose -f <repo>/docker-compose.yml \
     -f /tmp/tp-probe/compose-be-swap-override.yml exec -T doris-fe mysql -- 38040
bash probe-t-separator.sh standing-3.1.4 docker exec -i doris-2.1 mysql -- 8040
```

## 结论（两版本一致，均为 2026-10-30 实测）

1. **DECIMAL(18,2) × json NUMBER：大值丢 0.01（IEEE754）**——`99999999999999.99`
   落库成 `100000000000000.00`（double 最近值；`12.34`/`0.10`/`-5.01` 守恒，
   `12.345` 四舍五入 `12.35`）。
2. **DECIMAL(18,2) × json STRING（引擎实际发射形态）：全量守恒**——含 18,2 上限大值
   `99999999999999.99` 逐分不差。引擎 DorisSink 对 decimal 列走 `_` 回退臂
   （`tailrace-conn-doris/src/lib.rs` ArrayFormatter → `serde_json::to_string`），
   字符串形态恰好绕开了 double 解析。**若将来给 Decimal 加专门 arm 改发
   json number，本探针第 1 条就是它要踩的坑**——e2e 的 T 档大值断言锁这一前提。
3. **sequence_col 定序**：带 `"function_column.sequence_col"="seq"` 属性的 UNIQUE MoW
   表，partial 写低 seq=5（`sequence_column` 头在场）被丢弃（首写 seq=10 值全保留）；
   无属性表同载荷被覆盖（`K1 5 555`）——dogfooding #3 失效形态在 2.1.7 与 3.1.4
   均复现，W4 生成器补属性后引擎链路的定序保护才真实存在。
4. **DATETIME(6)/DATE**：空格与 RFC3339 **T 分隔**两种字符串形态都解析、微秒全保
   （`2026-10-30T12:34:56.789012` → `2026-10-30 12:34:56.789012`）；DATE 守恒。

## 工序注记

- 首轮 compose 探针曾把 `-P39030`（宿主映射口）传给容器内 mysql 而整轮失败
  ——`docker compose exec` 在容器内执行，mysql 端口恒 9030（partial_update.sh
  的 `doris_exec` 一直硬编码 9030 的原因）；修后重跑。
- `SELECT VERSION()` 返回的是 mysql 协议版本 5.7.99；服务端真实版本以
  `SHOW FRONTENDS`（doris-3.1.4-rc02 / 2.1.7）为准。
