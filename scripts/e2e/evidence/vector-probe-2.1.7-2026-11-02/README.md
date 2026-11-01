# ARRAY<FLOAT> probe — compose 2.1.7 (T-V6, R9 deferred item, 2026-11-02)

The R9 W2 probe (`../vector-probe-2026-10-30/`) ran standing-3.1.4 only —
the compose 2.1.7 stack was a 10-minute detour that wave, so this rerun on
2.1.7 was deferred ("Re-run on 2.1.7 when that stack is up"). T-V6 brings
the stack up (Doris fe-2.1.7 / be-2.1.7, `doris-2.1.7-rc03-443e87e203`)
and runs the SAME `probe.sh` (byte-identical copy) against it, probe-first
as usual, before the `variant_vector.sh` e2e compose branch below.

```
bash probe.sh compose-2.1.7 docker compose exec -T doris-fe mysql -- 38040
# → probe-compose-2.1.7.out
```

Stack bring-up notes (first 2.1.7 run on this host since the image rename):
the BE exits cleanly twice before it stays up — its installer checks
`vm.max_map_count >= 2000000` and refuses swap; the OrbStack docker VM
boots with neither satisfied, so both are set VM-globally from a
privileged one-shot container
(`docker run --rm --privileged --entrypoint sysctl apache/doris:be-2.1.7
-w vm.max_map_count=2000000`, then `swapoff -a` + `vm.overcommit_memory=1`
the same way). After that `docker compose up -d --wait doris-be` goes
healthy (7.82 GB VM, BE mem limit 7.03 GB).

## What the output says (2.1.7) — item-by-item against the 3.1.4 record

**Identical across the whole matrix.** No 2.1.7-specific surprise exists
on the ARRAY<FLOAT> path:

- **A — DDL shapes.** A1 (DUPLICATE + value column, the dim COMMENT kept
  verbatim) and A2 (UNIQUE MoW value column) create; A3 as a KEY column is
  refused with the exact legacy wording (`Array can only be used in the
  non-key column of the duplicate table at present.`); A4 as RANGE
  partition column refused (`Complex type column can't be partition
  column: …`). Display-only difference: 2.1.7 spells the type in the A4
  refusal lowercase (`array<float>`) where 3.1.4 printed `ARRAY<FLOAT>` —
  message wording a generator must NOT match on (the key heuristic keys on
  the errCode/refusal, not the spelling).
- **A5 — spelling round trip.** DESC prints `array<float>` back, COMMENT
  `vector dim=3` preserved — same infer fold (`array<float>` →
  `list<float32>`, dim stays declaration-only) as on 3.1.4.
- **B — load shapes.** B1/B2/B3 land the same array from all three forms —
  including B3, the spaced JSON string literal `"[0.1, 0.2, 0.3]"` THE
  SINK EMITS TODAY (no list-aware emission arm needed on 2.1.7 either).
  B4 integers widen to FLOAT, B5 null/absent → NULL array, B6 `[]` lands
  empty, B9 longer-than-dim-comment array accepted.
- **B7/B8/B10 — silent degrade.** A non-numeric element, a nested array
  and a scalar where the array goes all load with
  `NumberFilteredRows: 0` and degrade to NULL elements / NULL array
  (`[null, 2, 3]`, `[null, null]`, `NULL`) — same as 3.1.4, so the
  decoder's make-it-Bad guard (blueprint "报错不静默") is equally
  required against 2.1.7.
- **C — UNIQUE MoW × partial_columns.** Sibling-column partial update
  preserves the array, array-column partial update replaces it whole
  (C2 shows `[9, 9, 9]` — 9.0 in integer spelling; per-element shortest-
  display comparison, the known cross-version display variance, handles
  it).

**Verdict:** the R9 W4 conclusions (sink needs no List arm; generator key
heuristic steps over list columns; decoder dim guard is the only guard)
were drawn on 3.1.4 and hold verbatim on 2.1.7. The e2e that pins them
end-to-end runs next (`../../evidence/variant-vector-e2e-2.1.7-2026-11-02/`).
