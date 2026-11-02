# ARRAY<FLOAT> probe — R9 W2 / T-A5 (2026-10-30)

Bare stream loads at the BE, no engine in the path (same method as the
variant probe one directory over). Pins what Doris itself does with an
`ARRAY<FLOAT>` column before the generator's `list<float32[,dim]>` mapping
and the decoder's list path were written. **Standing 3.1.4 only** — the
compose 2.1.7 stack was not brought up for this wave (the BE-image swap of
the variant probe is a 10-minute detour; the ARRAY shapes below are
long-stable Doris behaviour and the W4 e2e runs on 3.1.4). Re-run on 2.1.7
when that stack is up: `bash probe.sh compose-2.1.7 docker compose … exec -T
doris-fe mysql -- 38040`.

```
bash probe.sh standing-3.1.4 docker exec -i doris-2.1 mysql -- 8040   # → probe-standing-3.1.4.out
```

## What the output says (3.1.4-rc02)

**A — DDL shapes.** `ARRAY<FLOAT>` creates as a value column under
DUPLICATE (A1, with a column `COMMENT 'vector dim=3'` — the dim annotation
the generator emits — kept verbatim by SHOW CREATE TABLE) and under UNIQUE
merge-on-write (A2). The FE refuses it as a KEY column (A3: `Array can only
be used in the non-key column of the duplicate table at present.` — legacy
wording, A2 shows the value-column case works under UNIQUE too) and as a
RANGE partition column (A4: `Complex type column can't be partition column:
ARRAY<FLOAT>`). So the generator's key heuristic must step over list columns
exactly as it steps over variant ones, and `range_key_capable` stays false.

**A5 — spelling round trip.** DESC prints the type back as `array<float>`;
the infer fold therefore maps `array<float>` → `list<float32>` (the dim is
declaration-only and not recoverable from DESC — a `list<float32,768>`
declaration diffs against the fold as a retype; see the W4 note below).

**B — load shapes (DUPLICATE table).** Three shapes land the same array
`[0.1, 0.2, 0.3]`: a bare JSON array (B1), a JSON *string literal* of the
compact array (B2), and a JSON string literal **with spaces after the
commas** (B3, `"[0.1, 0.2, 0.3]"`) — B3 is exactly what the engine's
DorisSink emits today for a List cell (no List arm; the ArrayFormatter
catch-all displays the arrow list and `serde_json::to_string` quotes it), so
**the sink needs no list-aware emission arm** — same conclusion as the
variant probe's B2. Integers widen to FLOAT (B4), `null` and an absent key
land NULL (B5), `[]` lands an empty array (B6), and a 5-element array into a
column whose comment says dim=3 loads fine (B9) — **Doris has no dimension;
only the engine can guard it.**

**B7/B8/B10 — why the engine-side guard is strict.** Doris does NOT reject
malformed vectors, it silently degrades them: a non-numeric element becomes
a NULL element (`["a",2,3]` → `[null, 2, 3]`), a nested array becomes NULL
elements (`[[1,2],[3]]` → `[null, null]`), a scalar where the array goes
becomes a NULL array (`42` → `NULL`) — all with `NumberFilteredRows: 0`. A
vector with a hole, or a vector that vanished, would be invisible downstream.
That is the evidence behind the decoder's choice to make such rows **Bad**
(bad-row policy: DLQ / fail) instead of mirroring the server's degrade —
blueprint "报错不静默".

**C — UNIQUE MoW × partial_columns.** A partial update of a sibling column
preserves the array (C1); a partial update of the array column replaces it
whole (C2). Same as the variant finding.

## W4 (e2e) notes

- Sink: the ArrayFormatter path is sufficient for the wire (B3). The e2e
  should still pin one f32 edge display (e.g. `1e-7`, arrow may print it in
  scientific notation — Doris' string→FLOAT cast handles `1e-7`, but assert
  it rather than assume).
- Evolve: `array<float>` folds to `list<float32>`; a dimensioned declaration
  `list<float32,N>` diffs as a retype against it (the diff compares vocabulary
  strings exactly). Either widen the console diff to treat the dim as
  declaration-only, or document the 409 suggestion as expected for vector
  columns — a console-side decision, not taken in this wave.
