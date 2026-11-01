# variant_vector e2e — compose 2.1.7 (T-V6, R9 deferred item, 2026-11-02)

The R10 recertification of this e2e ran standing-3.1.4
(`../variant-vector-e2e-2026-10-31/e2e-standing-3.1.4-run5-46of46.log`).
The compose 2.1.7 branch had never run — T-V6 closes that gap, probe
first (`../vector-probe-2.1.7-2026-11-02/`, byte-identical probe.sh,
matrix identical to 3.1.4), then the full e2e on the compose stack
(`doris-2.1.7-rc03-443e87e203`, fe 39030 / be 38040, kafka not started,
surgical teardown).

```
cargo build --release --locked -p tailrace-console -p tailrace-node   # no binaries on disk
bash scripts/e2e/variant_vector.sh                                    # compose mode, default ports
# → e2e-compose-2.1.7-run2.log : 47/47 steps passed
```

## run1 (kept, environmental)

`e2e-compose-2.1.7.log` — first invocation died at P0.1
(`available backend num is 0`): the e2e's BE-swap-check override file
recreates the BE container on first apply (this host's stack had been
brought up bare for the probe), and the FE's backend-availability
accounting (heartbeat + capacity report) was not yet settled when the
tier-P DDL fired. `step fail` records without exiting, so the run then
burned its be_write_gate retries against a nonexistent table; it was
stopped and rerun once the FE accepted DDL again (create-table probe
replied silently). run2's `up` shows `Running` (no recreate) — the
override had already converged. Kept as the record of the one
bring-up wrinkle, same convention as the 2026-10-31 evidence set.

## run2 — 47/47 on 2.1.7

Every tier green; highlights that matter against the 3.1.4 record:

- **P tier** — VARIANT + ARRAY<FLOAT> side by side: the sink's emission
  forms (variant as JSON string literal, vector as the arrow display
  string `"[0.1, 0.2, 0.3]"`) land identically to bare shapes; the f32
  edge values (`1e-7` / `0.33333334` / `3.4028235e38`) round-trip on
  2.1.7 as they did on 3.1.4.
- **T tier** — draft asserts `VARIANT` ×2 + `ARRAY<FLOAT> COMMENT 'vector
  dim=3'` + bare `ARRAY<FLOAT>`; keyColumn on variant/vector → 400
  (generator refusal, FE-equivalent semantics); the 5-row CORE load's
  nested-JSON and per-element array assertions pass; the dim-guard
  negatives land exactly 2 good rows, 3 bad rows in the node-state DLQ
  (byte-identical to the seeds after rawB64 decode),
  `tailrace_dlq_rows_total` = 3.
- **C tier (carrier proxy)** — the engine's real stream-load bytes show
  meta/content as whole-document JSON strings (no leaf parsing) and
  vectors as arrow display strings — the exact wire shapes the probe
  proved 2.1.7 parses.
- **Teardown** surgical (stop+rm doris-fe+doris-be only; compose kafka
  untouched).

**Verdict:** the R9 W4 behaviour pins (sink needs no list arm, decoder
dim guard is the only guard, variant passthrough) hold end-to-end on
Doris 2.1.7 as well as 3.1.4 — the deferred re-verify is closed, no
[skip] needed. Stack bring-up on this host additionally required the
docker-VM-level fixes recorded in the probe README (vm.max_map_count,
swapoff) — environment, not engine.
