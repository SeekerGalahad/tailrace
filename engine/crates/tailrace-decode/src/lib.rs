//! tailrace-decode: Decoder trait (bytes → Arrow columns, written straight
//! into column builders — no row-shaped intermediate) plus ogg/json/csv/
//! parquet/debezium-json decoders.
//!
//! ogg (Oracle GoldenGate CDC envelopes) is a generic adapter name: the payload
//! carries `after` + three envelope columns (op_type/ogg_table/ogg_ts).
//! debezium-json (blueprint C narrow path) consumes Debezium CDC events from
//! Kafka: `after.*` is the row data; op=d / `after`:null / tombstone (null
//! record → empty payload) skip; an event object without `after` goes Bad.
//! Cross-sample typing is carried by the target schema (this crate only writes
//! declared schema columns; inference belongs to the console discover step).
//! parquet brings its own schema: the whole file is one batch, column-cast
//! into the target via `ColBatch::append_batch`.

use datafusion::arrow::array::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder, Float64Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use simd_json::prelude::*;

/// Micro-batch column builder set (Arrow has no MutableRecordBatch equivalent
/// at this layer — thin wrapper).
pub struct ColBatch {
    schema: SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>,
}

impl ColBatch {
    pub fn new(schema: SchemaRef, rows: usize) -> Self {
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            let b: Box<dyn ArrayBuilder> = match f.data_type() {
                DataType::Utf8 => Box::new(StringBuilder::with_capacity(
                    rows.min(2000),
                    rows.min(2000) * 1100,
                )),
                DataType::Int64 => Box::new(Int64Builder::with_capacity(rows)),
                DataType::Float64 => Box::new(Float64Builder::with_capacity(rows)),
                DataType::Boolean => Box::new(BooleanBuilder::with_capacity(rows)),
                DataType::Date64 => Box::new(Date64Builder::with_capacity(rows)),
                // G2 decimal(p,s) (found by the R8 type-precision e2e: this
                // arm used to be the `unimplemented!` panic and took the
                // node down on the first decimal row). The builder carries
                // the declared (p,s) — without with_precision_and_scale,
                // finish() would emit Decimal128(38,10) and the batch would
                // fail schema validation. The scale also rides in every
                // append path (append_decimal_json / the CSV arm); a
                // scale-less append would corrupt silently
                DataType::Decimal128(p, s) => Box::new(
                    Decimal128Builder::with_capacity(rows)
                        .with_precision_and_scale(*p, *s)
                        .expect("schema-declared decimal(p,s) is G2-valid"),
                ),
                // G2 timestamp(unit) — arrow Timestamp(_, None): naive UTC.
                // Four units = four concrete builder types (arrow has no
                // unit-generic primitive builder)
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Second, None) => {
                    Box::new(TimestampSecondBuilder::with_capacity(rows))
                }
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None) => {
                    Box::new(TimestampMillisecondBuilder::with_capacity(rows))
                }
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None) => {
                    Box::new(TimestampMicrosecondBuilder::with_capacity(rows))
                }
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None) => {
                    Box::new(TimestampNanosecondBuilder::with_capacity(rows))
                }
                // G2 date — arrow Date32 (days since epoch)
                DataType::Date32 => Box::new(Date32Builder::with_capacity(rows)),
                other => unimplemented!("decode column type {other:?}"),
            };
            builders.push(b);
        }
        Self { schema, builders }
    }

    pub fn column_mut(&mut self, i: usize) -> &mut dyn ArrayBuilder {
        self.builders[i].as_mut()
    }

    /// Split borrow: (schema fields, column builders). The schema's field
    /// list IS the construct-time name cache — this hands both halves out
    /// simultaneously so the decode loop can iterate column names **by
    /// reference** while appending into the builders (T3: the old path
    /// cloned a `Vec<String>` of all column names per row to work around
    /// the borrow conflict).
    pub fn parts(
        &mut self,
    ) -> (
        &[datafusion::arrow::datatypes::FieldRef],
        &mut [Box<dyn ArrayBuilder>],
    ) {
        (self.schema.fields(), &mut self.builders)
    }

    pub fn len(&self) -> usize {
        self.builders.first().map(|b| b.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> ArrowResult<RecordBatch> {
        let arrays = self.builders.iter_mut().map(|b| b.finish()).collect();
        RecordBatch::try_new(self.schema.clone(), arrays)
    }

    /// Append a whole batch (formats that carry their own schema, e.g.
    /// parquet): source columns are cast to the target column type; missing
    /// source columns become null. Returns the appended row count. Cast
    /// failure is a runtime error (unreachable type combination — fail fast).
    pub fn append_batch(&mut self, rb: &RecordBatch) -> anyhow::Result<usize> {
        let rows = rb.num_rows();
        for (i, f) in self.schema.fields().iter().enumerate() {
            let src = rb.column_by_name(f.name());
            match src {
                None => append_nulls(&mut self.builders[i], rows),
                Some(arr) => {
                    let owned;
                    let casted: &dyn datafusion::arrow::array::Array = if arr.data_type()
                        == f.data_type()
                    {
                        arr.as_ref()
                    } else {
                        owned =
                            datafusion::arrow::compute::cast(arr, f.data_type()).map_err(|e| {
                                anyhow::anyhow!(
                                    "parquet column {} cast {:?}→{:?} failed: {e}",
                                    f.name(),
                                    arr.data_type(),
                                    f.data_type()
                                )
                            })?;
                        owned.as_ref()
                    };
                    append_array(&mut self.builders[i], casted);
                }
            }
        }
        Ok(rows)
    }
}

/// Append per target column type (post-cast: utf8/int64/float64/bool/date64
/// plus the G2 shapes since R8 — the cast in `append_batch` already
/// resolved scale/unit, so these are plain copies).
fn append_array(b: &mut dyn ArrayBuilder, arr: &dyn datafusion::arrow::array::Array) {
    use datafusion::arrow::array::*;
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        if let Some(sb) = b.as_any_mut().downcast_mut::<StringBuilder>() {
            for i in 0..a.len() {
                sb.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        if let Some(ib) = b.as_any_mut().downcast_mut::<Int64Builder>() {
            for i in 0..a.len() {
                ib.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
            for i in 0..a.len() {
                fb.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
            for i in 0..a.len() {
                bb.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
        if let Some(db) = b.as_any_mut().downcast_mut::<Decimal128Builder>() {
            for i in 0..a.len() {
                db.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
            for i in 0..a.len() {
                db.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
        }
    }
    // G2: date32 + the four timestamp units (parquet/batch path — the cast
    // in append_batch has already resolved the unit, this is a plain copy)
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        if let Some(db) = b.as_any_mut().downcast_mut::<Date32Builder>() {
            for i in 0..a.len() {
                db.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        if let Some(b2) = b.as_any_mut().downcast_mut::<TimestampSecondBuilder>() {
            for i in 0..a.len() {
                b2.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        if let Some(b2) = b.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>() {
            for i in 0..a.len() {
                b2.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        if let Some(b2) = b.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() {
            for i in 0..a.len() {
                b2.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
            return;
        }
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        if let Some(b2) = b.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>() {
            for i in 0..a.len() {
                b2.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
            }
        }
    }
}

fn append_nulls(b: &mut dyn ArrayBuilder, rows: usize) {
    use datafusion::arrow::array::*;
    if b.as_any_mut().downcast_mut::<StringBuilder>().is_some() {
        for _ in 0..rows {
            append_str(b, None);
        }
    } else if b.as_any_mut().downcast_mut::<Int64Builder>().is_some() {
        for _ in 0..rows {
            append_i64_opt(b, None);
        }
    } else if b.as_any_mut().downcast_mut::<Float64Builder>().is_some() {
        for _ in 0..rows {
            if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
                fb.append_null();
            }
        }
    } else if b.as_any_mut().downcast_mut::<BooleanBuilder>().is_some() {
        for _ in 0..rows {
            if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
                bb.append_null();
            }
        }
    } else if b.as_any_mut().downcast_mut::<Date64Builder>().is_some() {
        for _ in 0..rows {
            if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
                db.append_null();
            }
        }
    } else if b.as_any_mut().downcast_mut::<Decimal128Builder>().is_some() {
        for _ in 0..rows {
            if let Some(db) = b.as_any_mut().downcast_mut::<Decimal128Builder>() {
                db.append_null();
            }
        }
    } else if b.as_any_mut().downcast_mut::<Date32Builder>().is_some()
        || b.as_any_mut()
            .downcast_mut::<TimestampSecondBuilder>()
            .is_some()
        || b.as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .is_some()
        || b.as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .is_some()
        || b.as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .is_some()
    {
        for _ in 0..rows {
            append_ts_null(b);
            if let Some(db) = b.as_any_mut().downcast_mut::<Date32Builder>() {
                db.append_null();
            }
        }
    }
}

/// Append helper family. Semantics: append by the column's **real builder
/// type** (numbers into utf8 columns become strings; strings into numeric
/// columns parse-or-null) — every value is appended exactly once per column,
/// so equal column lengths are an invariant. (A past regression silently
/// dropped the column when the downcast failed, leaving mismatched lengths.)
fn append_str(b: &mut dyn ArrayBuilder, v: Option<&str>) {
    if let Some(sb) = b.as_any_mut().downcast_mut::<StringBuilder>() {
        sb.append_option(v);
    } else if let Some(ib) = b.as_any_mut().downcast_mut::<Int64Builder>() {
        ib.append_option(v.and_then(|s| s.parse::<i64>().ok()));
    } else if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
        fb.append_option(v.and_then(|s| s.parse::<f64>().ok()));
    } else if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
        bb.append_option(v.and_then(|s| s.parse::<bool>().ok()));
    } else if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
        db.append_option(v.and_then(|s| s.parse::<i64>().ok()));
    }
}

fn append_i64(b: &mut dyn ArrayBuilder, v: i64) {
    append_i64_opt(b, Some(v));
}

fn append_i64_opt(b: &mut dyn ArrayBuilder, v: Option<i64>) {
    if let Some(ib) = b.as_any_mut().downcast_mut::<Int64Builder>() {
        ib.append_option(v);
    } else if let Some(sb) = b.as_any_mut().downcast_mut::<StringBuilder>() {
        sb.append_option(v.map(|n| n.to_string()).as_deref());
    } else if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
        fb.append_option(v.map(|n| n as f64));
    } else if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
        bb.append_option(v.map(|n| n != 0));
    } else if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
        db.append_option(v);
    }
}

/// Float append. Cross-type semantics mirror the append_i64_opt family:
/// utf8 columns stringify, int64 columns truncate, bool columns are
/// nonzero-is-true. (A past regression left float values silently NULL after
/// both as_str and as_i64 failed on JSON floats.)
fn append_f64(b: &mut dyn ArrayBuilder, v: f64) {
    if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
        fb.append_value(v);
    } else if let Some(sb) = b.as_any_mut().downcast_mut::<StringBuilder>() {
        sb.append_value(v.to_string());
    } else if let Some(ib) = b.as_any_mut().downcast_mut::<Int64Builder>() {
        ib.append_value(v as i64);
    } else if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
        bb.append_value(v != 0.0);
    } else if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
        db.append_value(v as i64);
    }
}

/// Bool append (same regression family as append_f64: JSON true/false used to
/// fall through to NULL).
fn append_bool(b: &mut dyn ArrayBuilder, v: bool) {
    if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
        bb.append_value(v);
    } else if let Some(sb) = b.as_any_mut().downcast_mut::<StringBuilder>() {
        sb.append_value(if v { "true" } else { "false" });
    } else if let Some(ib) = b.as_any_mut().downcast_mut::<Int64Builder>() {
        ib.append_value(if v { 1 } else { 0 });
    } else if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
        fb.append_value(if v { 1.0 } else { 0.0 });
    } else if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
        db.append_null();
    }
}

// ==================== decimal(p,s) columns (G2) ====================
//
// Found by the R8 type-precision e2e: the json decoder met its first
// decimal(18,2) column and died on `ColBatch::new`'s unimplemented arm.
// The semantics below are the probed server contract
// (scripts/e2e/evidence/type-precision-probe-2026-10-30/), mirrored on the
// engine side:
//
//   * STRING cells parse as exact base-10 and NEVER pass through f64 — the
//     18,2 ceiling 99999999999999.99 must survive bit-exact (probe P1: the
//     string shape round-trips to the cent; that is the engine's own sink
//     emission shape too, which is why the whole chain conserves).
//   * NUMBER cells ride f64 (simd-json's parsed value; the original literal
//     is gone). At the 18,2 ceiling the double has already drifted +0.01 —
//     exactly what Doris itself does to a json NUMBER (probe P2), so the
//     engine is consistent with the server, and operators who care write
//     string cells (dogfooding round 2 friction note).
//   * extra fraction digits round half-up ("12.345" at s=2 → 1235; the
//     server rounds the same way).

/// Exact base-10 → unscaled i128 at the column scale, half-up on extra
/// fraction digits. Zero dependencies, overflow-checked (an i128 overflows
/// only beyond decimal(38,0), outside the G2 vocabulary).
fn parse_decimal_i128(s: &str, scale: u8) -> Option<i128> {
    let s = s.trim();
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
        || (int_part.is_empty() && frac_part.is_empty())
    {
        return None;
    }
    let mut acc: i128 = 0;
    for b in int_part
        .bytes()
        .chain(frac_part.bytes().take(scale as usize))
    {
        acc = acc.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
    }
    // pad a short fraction up to the scale (".5" at s=2 → 50)
    for _ in (frac_part.len() as u8)..scale {
        acc = acc.checked_mul(10)?;
    }
    // half-up on the first dropped fraction digit
    if frac_part
        .as_bytes()
        .get(scale as usize)
        .is_some_and(|b| b - b'0' >= 5)
    {
        acc = acc.checked_add(1)?;
    }
    Some(if neg { -acc } else { acc })
}

/// Append one JSON cell into a decimal column. Dispatched from every json
/// walk (jsonl / ogg after / debezium after) where the field's scale is at
/// hand; the generic append_* family cannot take it (their signatures are
/// scale-less by design — that family serves the five legacy shapes).
fn append_decimal_json(
    b: &mut dyn ArrayBuilder,
    v: Option<&simd_json::BorrowedValue<'_>>,
    scale: u8,
) {
    let Some(db) = b.as_any_mut().downcast_mut::<Decimal128Builder>() else {
        return;
    };
    let Some(v) = v else {
        db.append_null();
        return;
    };
    if let Some(s) = v.as_str() {
        db.append_option(parse_decimal_i128(s, scale));
    } else if let Some(iv) = v.as_i64() {
        // scale ≤ 38 (G2 vocabulary), and 10^38 < i128::MAX — no overflow
        db.append_option((iv as i128).checked_mul(10i128.pow(u32::from(scale))));
    } else if let Some(fv) = v.as_f64() {
        db.append_option(Some((fv * 10f64.powi(scale as i32)).round() as i128));
    } else {
        // JSON null / bool / nested — same treatment the typed chain gives
        // the other numeric shapes
        db.append_null();
    }
}

// ==================== timestamp(unit) / date columns (G2) ====================
//
// The e2e that surfaced the decimal gap walks all THREE G2 words the R8
// acceptance names (decimal/timestamp/date), so their decode face lands
// together. Naive wall-clock semantics (arrow Timestamp(_, None)): the
// parsed string is taken as UTC — Doris DATETIME has no timezone either,
// and the sink emits the value back as a formatted string, so the round
// trip is identity. Formats accepted: "YYYY-MM-DD[ HH:MM:SS[.frac]]" with
// space or RFC3339 'T' separator (both parse on the server side too —
// probed 2026-10-30); everything else is null, not a guess.

/// Days since 1970-01-01 from a civil date (Howard Hinnant's algorithm —
/// zero dependencies, exact for the proleptic Gregorian calendar).
fn days_from_civil(y: i64, m: i64, d: i64) -> Option<i64> {
    if !(1..=12).contains(&m) {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=dim).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146097 + doe - 719468)
}

fn unit_factor(unit: &datafusion::arrow::datatypes::TimeUnit) -> i64 {
    match unit {
        datafusion::arrow::datatypes::TimeUnit::Second => 1,
        datafusion::arrow::datatypes::TimeUnit::Millisecond => 1_000,
        datafusion::arrow::datatypes::TimeUnit::Microsecond => 1_000_000,
        datafusion::arrow::datatypes::TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Narrow ISO parser → epoch value in the column's unit. Fraction digits
/// pad with zeros / truncate at the unit's width (no rounding — a wider
/// literal in a narrower column is a producer-side mismatch, and the server
/// side keeps whatever fits too).
fn parse_timestamp_i64(s: &str, unit: &datafusion::arrow::datatypes::TimeUnit) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = match s.split_once([' ', 'T', 't']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let (y, rest) = date_part.split_once('-')?;
    let (m, d) = rest.split_once('-')?; // exactly YYYY-M-D after two splits
    let (y, m, d): (i64, i64, i64) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    let days = days_from_civil(y, m, d)?;
    let mut secs = days * 86_400;
    let mut frac_units: i64 = 0;
    if let Some(t) = time_part {
        let (hms, frac) = match t.split_once('.') {
            Some((h, f)) => (h, Some(f)),
            None => (t, None),
        };
        let (h, rest) = hms.split_once(':')?;
        let (mi, se) = rest.split_once(':')?;
        let (h, mi, se): (i64, i64, i64) = (h.parse().ok()?, mi.parse().ok()?, se.parse().ok()?);
        if !(0..24).contains(&h) || !(0..60).contains(&mi) || !(0..60).contains(&se) {
            return None;
        }
        secs += h * 3_600 + mi * 60 + se;
        if let Some(f) = frac {
            if !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let width = match unit {
                datafusion::arrow::datatypes::TimeUnit::Second => 0,
                datafusion::arrow::datatypes::TimeUnit::Millisecond => 3,
                datafusion::arrow::datatypes::TimeUnit::Microsecond => 6,
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => 9,
            };
            let mut padded = f.to_string();
            if padded.len() > width {
                padded.truncate(width);
            }
            while padded.len() < width {
                padded.push('0');
            }
            if !padded.is_empty() {
                frac_units = padded.parse().ok()?;
            }
        }
    }
    Some(secs * unit_factor(unit) + frac_units)
}

/// One JSON cell into a timestamp(unit) column: strings parse via the
/// narrow ISO reader, integers ride through as epoch-in-unit, floats are
/// seconds scaled.
fn append_timestamp_json(
    b: &mut dyn ArrayBuilder,
    v: Option<&simd_json::BorrowedValue<'_>>,
    unit: &datafusion::arrow::datatypes::TimeUnit,
) {
    use datafusion::arrow::array::{
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder,
    };
    let Some(v) = v else { return append_ts_null(b) };
    if v.is_null() {
        return append_ts_null(b);
    }
    if let Some(s) = v.as_str() {
        append_ts_str(b, Some(s), unit);
        return;
    }
    let secs = if let Some(iv) = v.as_i64() {
        iv // already in the column's unit
    } else if let Some(fv) = v.as_f64() {
        (fv * unit_factor(unit) as f64).round() as i64
    } else {
        return append_ts_null(b);
    };
    match unit {
        datafusion::arrow::datatypes::TimeUnit::Second => b
            .as_any_mut()
            .downcast_mut::<TimestampSecondBuilder>()
            .unwrap()
            .append_value(secs),
        datafusion::arrow::datatypes::TimeUnit::Millisecond => b
            .as_any_mut()
            .downcast_mut::<TimestampMillisecondBuilder>()
            .unwrap()
            .append_value(secs),
        datafusion::arrow::datatypes::TimeUnit::Microsecond => b
            .as_any_mut()
            .downcast_mut::<TimestampMicrosecondBuilder>()
            .unwrap()
            .append_value(secs),
        datafusion::arrow::datatypes::TimeUnit::Nanosecond => b
            .as_any_mut()
            .downcast_mut::<TimestampNanosecondBuilder>()
            .unwrap()
            .append_value(secs),
    }
}

fn append_ts_null(b: &mut dyn ArrayBuilder) {
    use datafusion::arrow::array::{
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder,
    };
    if let Some(b) = b.as_any_mut().downcast_mut::<TimestampSecondBuilder>() {
        b.append_null();
    } else if let Some(b) = b.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>() {
        b.append_null();
    } else if let Some(b) = b.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() {
        b.append_null();
    } else if let Some(b) = b.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>() {
        b.append_null();
    }
}

/// One JSON cell into a date column (Date32, days since epoch):
/// "YYYY-MM-DD" strings, integers as days.
fn append_date32_json(b: &mut dyn ArrayBuilder, v: Option<&simd_json::BorrowedValue<'_>>) {
    let Some(db) = b.as_any_mut().downcast_mut::<Date32Builder>() else {
        return;
    };
    let Some(v) = v else {
        db.append_null();
        return;
    };
    if let Some(s) = v.as_str() {
        append_date32_str(b, Some(s));
    } else if let Some(iv) = v.as_i64() {
        db.append_option(i32::try_from(iv).ok());
    } else {
        db.append_null();
    }
}

/// String → timestamp column (json string cells and CSV cells share it).
fn append_ts_str(
    b: &mut dyn ArrayBuilder,
    v: Option<&str>,
    unit: &datafusion::arrow::datatypes::TimeUnit,
) {
    use datafusion::arrow::array::{
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder,
    };
    let parsed = v.and_then(|s| parse_timestamp_i64(s, unit));
    if let Some(b) = b.as_any_mut().downcast_mut::<TimestampSecondBuilder>() {
        b.append_option(parsed);
    } else if let Some(b) = b.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>() {
        b.append_option(parsed);
    } else if let Some(b) = b.as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>() {
        b.append_option(parsed);
    } else if let Some(b) = b.as_any_mut().downcast_mut::<TimestampNanosecondBuilder>() {
        b.append_option(parsed);
    }
}

/// String → date column ("YYYY-MM-DD" → days since epoch).
fn append_date32_str(b: &mut dyn ArrayBuilder, v: Option<&str>) {
    let days = v.and_then(|s| {
        let (y, rest) = s.trim().split_once('-')?;
        let (m, d) = rest.split_once('-')?;
        days_from_civil(y.parse().ok()?, m.parse().ok()?, d.parse().ok()?).map(|n| n as i32)
    });
    if let Some(db) = b.as_any_mut().downcast_mut::<Date32Builder>() {
        db.append_option(days);
    }
}

/// G2 typed cells (decimal/timestamp/date) — the shapes whose append needs
/// the column's type parameters, dispatched from every json walk before the
/// generic typed chain (which is shape-less). Returns false when the column
/// is none of these and the caller falls through.
fn append_g2_json(
    b: &mut dyn ArrayBuilder,
    dt: &DataType,
    v: Option<&simd_json::BorrowedValue<'_>>,
) -> bool {
    match dt {
        DataType::Decimal128(_, s) => {
            append_decimal_json(b, v, u8::try_from(*s).unwrap_or(0));
            true
        }
        DataType::Timestamp(unit, None) => {
            append_timestamp_json(b, v, unit);
            true
        }
        DataType::Date32 => {
            append_date32_json(b, v);
            true
        }
        _ => false,
    }
}

/// Decode outcome: Append=column-filled / Skip=not-a-row, dropped silently /
/// Bad=parse-or-shape failure (carrying the raw bytes back).
///
/// Skip sources (the runtime folds them into one `skipped` counter): pushdown
/// early filter (OGG op_type whitelist), CSV blank line, and CDC non-row
/// events — Debezium tombstone (null-valued record), delete (op=d) and
/// `after`:null events (delete / truncate `t` / message `m`).
#[derive(PartialEq, Debug)]
pub enum Decoded {
    Append,
    Skip,
    /// Parse failure. The raw bytes ride along (ownership round-trip: the
    /// caller's bad-row policy — DLQ quarantine or fail-sample — needs them
    /// after the decoder has consumed the buffer; handing them back here
    /// avoids re-cloning what the owned-`Vec<u8>` signature just removed).
    ///
    /// Byte-fidelity caveat (T3, measured): the in-place simd-json parse may
    /// already have rewritten escape sequences in strings it finished
    /// scanning before hitting the failure — a poisoned row like
    /// `{"ok":"\u0041","bad":` comes back with that escape rewritten. Plain
    /// malformed rows come back byte-identical. The pre-T3 scratch-copy
    /// design preserved the original bytes exactly at the cost of one full
    /// copy per **every** message; removing that copy on the happy path
    /// wins the trade (the DLQ sample stays quarantine-grade readable).
    Bad(Vec<u8>),
}

/// Decoder: bytes → straight into the target schema columns. pushdown = the
/// hook for compile-time constant-predicate early filtering.
///
/// The signature takes the buffer **by value** (T3 zero-copy): the JSON
/// decoders parse simd-json in place inside it, the parquet decoder wraps it
/// into `bytes::Bytes` — no per-message copy is made between the source's
/// payload and the parser.
///
/// `Sync` (T5 plan-a): the runtime fans one poll's chunks out to scoped
/// worker threads, each decoding into its own `ColBatch` — the decoder is
/// shared by reference across those threads. Every implementation is a
/// stateless unit struct (`decode` takes `&self` and touches no interior
/// mutability), so the bound is declarative only; no implementor changes.
pub trait Decoder: Send + Sync {
    fn decode(&self, raw: Vec<u8>, batch: &mut ColBatch, pushdown: &Pushdown) -> Decoded;
}

/// Constant-predicate pushdown (v1: equality whitelist extracted from SQL
/// predicates by the compiler).
#[derive(Default)]
pub struct Pushdown {
    /// column → allowed values (a row whose column value is outside the set
    /// is Skipped before any column copy)
    pub eq_whitelist: std::collections::HashMap<String, Vec<String>>,
}

// ==================== JSON (root object, direct key access) ====================

/// Where an inner decode walk stopped (mirrors `Decoded` minus the buffer —
/// the outer `decode` frame keeps owning the buffer and hands it back on the
/// Bad path, which is only sound because the parser's borrow of it ends when
/// the walk returns).
#[derive(PartialEq, Debug)]
enum WalkFail {
    Skip,
    Bad,
}

/// JSON decoder: root object keys write into same-named columns (schema
/// column names match JSON keys).
pub struct JsonDecoder;

impl JsonDecoder {
    /// Borrow-scoped walk: simd-json parses the buffer in place and the DOM's
    /// borrow of it stays inside this function (the owned-buffer `decode`
    /// frame below can then hand the buffer back on failure).
    fn walk(buf: &mut [u8], batch: &mut ColBatch) -> Result<(), WalkFail> {
        let dom = match simd_json::to_borrowed_value(buf) {
            Ok(v) => v,
            Err(_) => return Err(WalkFail::Bad),
        };
        // Typed extraction chain: as_str → as_i64 → as_f64 → as_bool → null.
        // (Floats and bools used to fall through to NULL when as_str/as_i64
        // both failed — whole float64/bool columns silently lost their values.)
        let (fields, builders) = batch.parts();
        for (i, f) in fields.iter().enumerate() {
            let name = f.name();
            // G2 typed columns (decimal/timestamp/date) carry their type
            // parameters — see append_g2_json
            if append_g2_json(builders[i].as_mut(), f.data_type(), dom.get(name.as_str())) {
                continue;
            }
            match dom.get(name.as_str()).and_then(|v| v.as_str()) {
                Some(s) => append_str(builders[i].as_mut(), Some(s)),
                None => {
                    let b = builders[i].as_mut();
                    if let Some(iv) = dom.get(name.as_str()).and_then(|v| v.as_i64()) {
                        append_i64(b, iv);
                    } else if let Some(fv) = dom.get(name.as_str()).and_then(|v| v.as_f64()) {
                        append_f64(b, fv);
                    } else if let Some(bv) = dom.get(name.as_str()).and_then(|v| v.as_bool()) {
                        append_bool(b, bv);
                    } else {
                        append_str(b, None);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Decoder for JsonDecoder {
    fn decode(&self, mut raw: Vec<u8>, batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        // simd-json mutates the buffer in place — the owned signature lets it
        // parse straight out of the source's payload (no scratch copy).
        match Self::walk(raw.as_mut_slice(), batch) {
            Ok(()) => Decoded::Append,
            Err(WalkFail::Bad) => Decoded::Bad(raw),
            Err(WalkFail::Skip) => Decoded::Skip, // unreachable for JSON; kept for the shared shape
        }
    }
}

// ==================== OGG (after + three envelope columns) ====================

/// OGG decoder: envelope columns first (op_type/ogg_table/ogg_ts), after.*
/// columns after — the column order matches the console schema inference.
pub struct OggDecoder;

impl OggDecoder {
    /// Borrow-scoped walk (same rationale as [`JsonDecoder::walk`]).
    fn walk(buf: &mut [u8], batch: &mut ColBatch, pushdown: &Pushdown) -> Result<(), WalkFail> {
        let dom = match simd_json::to_borrowed_value(buf) {
            Ok(v) => v,
            Err(_) => return Err(WalkFail::Bad),
        };
        let after = match dom.get("after") {
            Some(v) => v,
            None => return Err(WalkFail::Bad), // an OGG shape must have `after`
        };
        let (fields, builders) = batch.parts();
        // Pushdown early filter: op_type outside the whitelist → dropped with
        // zero column copies.
        if !pushdown.eq_whitelist.is_empty() {
            if let Some(allowed) = pushdown.eq_whitelist.get("op_type") {
                let op = dom.get("op_type").and_then(|v| v.as_str()).unwrap_or("");
                if !allowed.iter().any(|a| a == op) {
                    return Err(WalkFail::Skip);
                }
            }
        }
        for (i, f) in fields.iter().enumerate() {
            // G2 typed columns (decimal/timestamp/date) carry their type
            // parameters — see append_g2_json
            if append_g2_json(
                builders[i].as_mut(),
                f.data_type(),
                after.get(f.name().as_str()),
            ) {
                continue;
            }
            match f.name().as_str() {
                "op_type" => append_str(
                    builders[i].as_mut(),
                    dom.get("op_type").and_then(|v| v.as_str()),
                ),
                "ogg_table" => append_str(
                    builders[i].as_mut(),
                    dom.get("table").and_then(|v| v.as_str()),
                ),
                "ogg_ts" => append_i64(
                    builders[i].as_mut(),
                    dom.get("ts").and_then(|v| v.as_i64()).unwrap_or(0),
                ),
                name => match after.get(name).and_then(|v| v.as_str()) {
                    Some(s) => append_str(builders[i].as_mut(), Some(s)),
                    None => {
                        // Same typed chain as JsonDecoder: after.* floats and
                        // bools must not fall through to NULL.
                        let b = builders[i].as_mut();
                        if let Some(iv) = after.get(name).and_then(|v| v.as_i64()) {
                            append_i64(b, iv);
                        } else if let Some(fv) = after.get(name).and_then(|v| v.as_f64()) {
                            append_f64(b, fv);
                        } else if let Some(bv) = after.get(name).and_then(|v| v.as_bool()) {
                            append_bool(b, bv);
                        } else {
                            append_str(b, None);
                        }
                    }
                },
            }
        }
        Ok(())
    }
}

impl Decoder for OggDecoder {
    fn decode(&self, mut raw: Vec<u8>, batch: &mut ColBatch, pushdown: &Pushdown) -> Decoded {
        match Self::walk(raw.as_mut_slice(), batch, pushdown) {
            Ok(()) => Decoded::Append,
            Err(WalkFail::Skip) => Decoded::Skip,
            Err(WalkFail::Bad) => Decoded::Bad(raw),
        }
    }
}

// ==================== Debezium JSON (CDC envelope, after.* as row data) ====================

/// Debezium JSON decoder (Kafka Connect JSON converter envelope, blueprint C
/// narrow path — consumes Debezium events from Kafka; tailrace does not
/// capture binlogs itself). Both converter shapes are accepted:
/// - schemas.enable=false (unwrapped):
///   `{"before":null,"after":{"id":1,"name":"foo"},"op":"c","ts_ms":123}`
/// - schemas.enable=true (Kafka Connect JsonConverter default): the same
///   event wrapped as `{"schema":{…},"payload":{…}}` — the payload is
///   unwrapped before envelope handling (both keys together identify the
///   wrapper; a bare Debezium envelope never carries either).
///
/// v1 row semantics:
/// - `after.*` keys write into same-named schema columns (typed chain
///   identical to JsonDecoder/OggDecoder; nested objects/arrays in a field
///   value decode as null, matching that family). `before`/`op`/`ts_ms`/
///   `source` are envelope metadata and are NOT decoded into columns (an
///   OGG-style op/ts envelope-column pair is a v1.x follow-up, as is console
///   discover inference for this format).
/// - Tombstone: Debezium (tombstones.on.delete=true) emits a **null-valued
///   Kafka record**, which the Kafka conn layer hands through as an empty
///   buffer — empty/whitespace-only input Skips (a bare JSON `null` message
///   is tolerated the same way, though Kafka never delivers it for a
///   tombstone). op=`d` (delete) and `after`:null events (delete / truncate
///   `t` / message `m`) also Skip — v1 carries no delete marker downstream.
/// - Malformed JSON, a non-object non-null root, an `after` that is a
///   non-object scalar/array, and — shape strictness — an event object with
///   NO `after` key (or a wrapper `payload` that is not an object) go Bad:
///   real c/r/u events always carry `after`, so a missing key means a
///   misrouted/misconfigured producer and must surface via the bad-row
///   policy (DLQ/fail) instead of silently vanishing.
/// - Numbers above i64::MAX (e.g. MySQL BIGINT UNSIGNED) decode as null —
///   shared typed-chain family behavior (Json/Ogg decoders alike).
///
/// Pushdown is ignored: the op_type-keyed whitelist targets the OGG envelope
/// column, which Debezium v1 does not decode.
pub struct DebeziumJsonDecoder;

impl DebeziumJsonDecoder {
    /// Borrow-scoped walk (same rationale as [`JsonDecoder::walk`]).
    fn walk(buf: &mut [u8], batch: &mut ColBatch) -> Result<(), WalkFail> {
        // Tombstone first: a null-valued Kafka record arrives as an EMPTY
        // buffer (the Kafka conn layer maps null payloads to `&[]`) — that is
        // a Skip, not a parse failure. Whitespace-only rides the same rule.
        if buf.iter().all(|b| b.is_ascii_whitespace()) {
            return Err(WalkFail::Skip);
        }
        let dom = match simd_json::to_borrowed_value(buf) {
            Ok(v) => v,
            Err(_) => return Err(WalkFail::Bad),
        };
        // Root must be the event object; a bare `null` is tolerated as a
        // tombstone (Skip), any other non-object root is not an event (Bad).
        if !dom.is_object() {
            return if dom.is_null() {
                Err(WalkFail::Skip)
            } else {
                Err(WalkFail::Bad)
            };
        }
        // schemas.enable=true wrapper: unwrap the payload (both keys together
        // identify the wrapper — a bare Debezium envelope carries neither).
        let env = if dom.get("schema").is_some() && dom.get("payload").is_some() {
            match dom.get("payload") {
                Some(p) if p.is_object() => p,
                Some(p) if p.is_null() => return Err(WalkFail::Skip), // degenerate wrapper
                _ => return Err(WalkFail::Bad),
            }
        } else {
            &dom
        };
        // Delete: after is null in a well-formed event — skip even an
        // out-of-shape `d` carrying an after object (v1 drops deletes).
        if env.get("op").and_then(|v| v.as_str()) == Some("d") {
            return Err(WalkFail::Skip);
        }
        // Real c/r/u events always carry `after` (null only for delete and
        // op t/m); an absent key is a misrouted/misconfigured producer — Bad,
        // so the bad-row policy (DLQ/fail) makes it visible instead of the
        // row silently vanishing.
        let after = match env.get("after") {
            None => return Err(WalkFail::Bad), // not a Debezium envelope
            Some(v) if v.is_null() => return Err(WalkFail::Skip),
            Some(v) if v.is_object() => v,
            Some(_) => return Err(WalkFail::Bad), // envelope violation: after must be an object
        };
        let (fields, builders) = batch.parts();
        for (i, f) in fields.iter().enumerate() {
            let name = f.name();
            // G2 typed columns (decimal/timestamp/date) carry their type
            // parameters — see append_g2_json
            if append_g2_json(
                builders[i].as_mut(),
                f.data_type(),
                after.get(name.as_str()),
            ) {
                continue;
            }
            match after.get(name.as_str()).and_then(|v| v.as_str()) {
                Some(s) => append_str(builders[i].as_mut(), Some(s)),
                None => {
                    // Same typed chain as JsonDecoder/OggDecoder: floats and
                    // bools must not fall through to NULL.
                    let b = builders[i].as_mut();
                    if let Some(iv) = after.get(name.as_str()).and_then(|v| v.as_i64()) {
                        append_i64(b, iv);
                    } else if let Some(fv) = after.get(name.as_str()).and_then(|v| v.as_f64()) {
                        append_f64(b, fv);
                    } else if let Some(bv) = after.get(name.as_str()).and_then(|v| v.as_bool()) {
                        append_bool(b, bv);
                    } else {
                        // absent key, JSON null, or nested object/array
                        append_str(b, None);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Decoder for DebeziumJsonDecoder {
    fn decode(&self, mut raw: Vec<u8>, batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        match Self::walk(raw.as_mut_slice(), batch) {
            Ok(()) => Decoded::Append,
            Err(WalkFail::Skip) => Decoded::Skip,
            Err(WalkFail::Bad) => Decoded::Bad(raw),
        }
    }
}

// ==================== CSV (RFC4180 via the csv crate, typed by target column) ====================

/// CSV decoder: one RFC4180 record per message, fields written by column
/// order (schema column order = file column order, matching the console csv
/// inference). Quoted fields carry commas, doubled quotes and in-field
/// newlines (`"a,b"`, `"he said ""hi"""`); the record grammar is delegated
/// to the `csv` crate — the old `split(',')` splitter had no quote handling
/// at all. Per-column coercion semantics are unchanged: bad numerics
/// degrade to null, the row is not dropped; positional fields are trimmed
/// (the splitter's whitespace tolerance).
pub struct CsvDecoder;

impl Decoder for CsvDecoder {
    fn decode(&self, raw: Vec<u8>, batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        let line = match std::str::from_utf8(&raw) {
            Ok(s) => s.trim_end(),
            Err(_) => return Decoded::Bad(raw),
        };
        if line.is_empty() {
            return Decoded::Skip;
        }
        // has_headers(false): the only record IS the data; flexible(true):
        // ragged records keep the positional mapping — missing trailing
        // fields decode as empty → null, extra ones ignored (same as the old
        // splitter's parts.get(i)). Note the csv reader is lenient on an
        // unterminated trailing quote (`a,"b` → fields a,b) — it decodes
        // rather than fails; anything harder (invalid UTF-8, mid-record
        // garbage) surfaces as Err → Bad, where the bad-row policy decides.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(line.as_bytes());
        let mut rec = csv::StringRecord::new();
        match rdr.read_record(&mut rec) {
            Ok(true) => {}
            Ok(false) => return Decoded::Skip, // defensive: empty input
            Err(_) => return Decoded::Bad(raw),
        }
        let (fields, builders) = batch.parts();
        for (i, f) in fields.iter().enumerate() {
            let v = rec.get(i).map(str::trim).unwrap_or("");
            let b = builders[i].as_mut();
            match f.data_type() {
                DataType::Int64 => match v.parse::<i64>() {
                    Ok(n) => append_i64(b, n),
                    Err(_) => append_i64_opt(b, None),
                },
                // decimal cells parse exact base-10 (see parse_decimal_i128)
                DataType::Decimal128(_, s) => {
                    if let Some(db) = b.as_any_mut().downcast_mut::<Decimal128Builder>() {
                        db.append_option(if v.is_empty() {
                            None
                        } else {
                            parse_decimal_i128(v, u8::try_from(*s).unwrap_or(0))
                        });
                    }
                }
                // G2 timestamp/date cells: the same narrow readers the json
                // walks use
                DataType::Timestamp(unit, None) => {
                    append_ts_str(b, if v.is_empty() { None } else { Some(v) }, unit);
                }
                DataType::Date32 => {
                    append_date32_str(b, if v.is_empty() { None } else { Some(v) });
                }
                DataType::Float64 => {
                    if let Some(fb) = b.as_any_mut().downcast_mut::<Float64Builder>() {
                        fb.append_option(v.parse::<f64>().ok());
                    }
                }
                DataType::Boolean => {
                    if let Some(bb) = b.as_any_mut().downcast_mut::<BooleanBuilder>() {
                        bb.append_option(v.parse::<bool>().ok());
                    }
                }
                _ => {
                    append_str(b, if v.is_empty() { None } else { Some(v) });
                }
            }
        }
        Decoded::Append
    }
}

// ==================== Parquet (whole file as one batch, brings its own schema) ====================

/// Parquet decoder: raw = whole file bytes (fed by the file source's batch
/// branch) → ParquetRecordBatchReader → `ColBatch::append_batch` (columns
/// cast into the target schema).
pub struct ParquetDecoder;

impl Decoder for ParquetDecoder {
    fn decode(&self, raw: Vec<u8>, batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        // `From<Vec<u8>>` wraps the allocation (refcount) — the old path
        // copied the whole file here.
        let bytes = bytes::Bytes::from(raw);
        let builder = match ParquetRecordBatchReaderBuilder::try_new(bytes) {
            Ok(b) => b,
            Err(_) => return Decoded::Bad(Vec::new()),
        };
        let reader = match builder.build() {
            Ok(r) => r,
            Err(_) => return Decoded::Bad(Vec::new()),
        };
        for rb in reader {
            match rb {
                Ok(rb) => {
                    if batch.append_batch(&rb).is_err() {
                        return Decoded::Bad(Vec::new());
                    }
                }
                Err(_) => return Decoded::Bad(Vec::new()),
            }
        }
        Decoded::Append
    }
}

impl ColBatch {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn ogg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("op_type", DataType::Utf8, false),
            Field::new("ogg_table", DataType::Utf8, true),
            Field::new("ogg_ts", DataType::Int64, true),
            Field::new("order_no", DataType::Utf8, true),
            Field::new("seq", DataType::Int64, true),
        ]))
    }

    #[test]
    fn ogg_decode_envelope_first_and_pushdown() {
        let mut batch = ColBatch::new(ogg_schema(), 8);
        let ogg = OggDecoder;
        let msgs: Vec<&[u8]> = vec![
            br#"{"table":"SHOPDB.ORDERS","op_type":"I","ts":1726000000001,"after":{"order_no":"ORD-1","seq":7}}"#,
            br#"{"table":"SHOPDB.ORDERS","op_type":"U","ts":1726000000002,"after":{"order_no":"ORD-2","seq":8}}"#,
            br#"{"table":"SHOPDB.ORDERS","op_type":"D","ts":1726000000003,"after":{"order_no":"ORD-3","seq":9}}"#,
        ];
        let mut pushdown = Pushdown::default();
        pushdown
            .eq_whitelist
            .insert("op_type".into(), vec!["I".into(), "U".into()]);
        let mut appended = 0;
        for m in msgs {
            if ogg.decode(m.to_vec(), &mut batch, &pushdown) == Decoded::Append {
                appended += 1;
            }
        }
        assert_eq!(appended, 2, "D rows dropped by pushdown early filter");
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 2);
        let op = rb
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(op.value(0), "I");
        let seq = rb
            .column(4)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(seq.value(1), 8);
    }

    /// G2 decimal(p,s): exact base-10 parse and the probed server semantics.
    /// Found live by the R8 type-precision e2e (the builder used to be the
    /// `unimplemented!` panic).
    #[test]
    fn parse_decimal_exact_and_half_up() {
        use super::parse_decimal_i128 as p;
        // plain cents, trailing zero, negative, short fraction padding
        assert_eq!(p("12.34", 2), Some(1234));
        assert_eq!(p("0.10", 2), Some(10));
        assert_eq!(p("-5.01", 2), Some(-501));
        assert_eq!(p(".5", 2), Some(50));
        assert_eq!(p("7", 2), Some(700));
        // the 18,2 ceiling survives bit-exact — never through f64
        assert_eq!(p("99999999999999.99", 2), Some(9999999999999999));
        // extra digits round half-up, like the server does
        assert_eq!(p("12.345", 2), Some(1235));
        assert_eq!(p("12.344", 2), Some(1234));
        // garbage is None (→ null), not a panic and not 0
        assert_eq!(p("", 2), None);
        assert_eq!(p("12,34", 2), None);
        assert_eq!(p("1.2.3", 2), None);
        assert_eq!(p("abc", 2), None);
    }

    #[test]
    fn json_decode_decimal_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amt",
            DataType::Decimal128(18, 2),
            true,
        )]));
        let mut batch = ColBatch::new(schema, 8);
        for m in [
            br#"{"amt":"99999999999999.99"}"#.as_slice(), // string: exact
            br#"{"amt":"12.34"}"#.as_slice(),
            br#"{"amt":12.34}"#.as_slice(), // number: f64 detour (fine at this magnitude)
            br#"{"amt":7}"#.as_slice(),     // integer: scaled
            br#"{"amt":null}"#.as_slice(),
        ] {
            assert_eq!(
                JsonDecoder.decode(m.to_vec(), &mut batch, &Pushdown::default()),
                Decoded::Append
            );
        }
        let rb = batch.finish().unwrap();
        let col = rb
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(col.value(0), 9999999999999999, "ceiling survives exactly");
        assert_eq!(col.value(1), 1234);
        assert_eq!(col.value(2), 1234);
        assert_eq!(col.value(3), 700, "integer scales by 10^s");
        assert!(col.is_null(4));
    }

    /// G2 timestamp(unit)/date: narrow ISO reader + json round trip. The
    /// space/T duality and microsecond retention mirror the probed server
    /// behaviour (type-precision probe C / T-separator mini probe).
    #[test]
    fn parse_timestamp_shapes() {
        use super::parse_timestamp_i64 as ts;
        use datafusion::arrow::datatypes::TimeUnit as U;
        // 2026-10-30 12:34:56.789012 in micros since epoch
        let expect_us = 1_793_363_696_789_012i64;
        assert_eq!(
            ts("2026-10-30 12:34:56.789012", &U::Microsecond),
            Some(expect_us)
        );
        assert_eq!(
            ts("2026-10-30T12:34:56.789012", &U::Microsecond),
            Some(expect_us),
            "RFC3339 T separator"
        );
        // unit scaling: same instant, wider/narrower units
        assert_eq!(
            ts("2026-10-30 12:34:56", &U::Second),
            Some(expect_us / 1_000_000)
        );
        assert_eq!(
            ts("2026-10-30 12:34:56.789", &U::Millisecond),
            Some(1_793_363_696_789)
        );
        // bare date = midnight; fraction pads/truncates at unit width
        assert_eq!(ts("2026-10-30", &U::Second), Some(1_793_318_400));
        assert_eq!(
            ts("2026-10-30 12:34:56.7890125", &U::Microsecond),
            Some(expect_us),
            "7th digit truncated"
        );
        // pre-epoch date stays negative-exact
        assert_eq!(ts("1969-12-31 23:59:59", &U::Second), Some(-1));
        // garbage → None
        assert_eq!(ts("2026-13-01 00:00:00", &U::Second), None, "month 13");
        assert_eq!(ts("2026-02-30", &U::Second), None, "feb 30");
        assert_eq!(ts("not a date", &U::Second), None);
        assert_eq!(ts("2026-10-30 25:00:00", &U::Second), None, "hour 25");
    }

    #[test]
    fn json_decode_timestamp_and_date_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("d", DataType::Date32, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        for m in [
            br#"{"ts":"2026-10-30 12:34:56.789012","d":"2026-10-30"}"#.as_slice(),
            br#"{"ts":"2026-10-30T00:00:00","d":"1970-01-01"}"#.as_slice(),
            br#"{"ts":1793363696,"d":20756}"#.as_slice(), // epoch secs / days
            br#"{"ts":null,"d":null}"#.as_slice(),
        ] {
            assert_eq!(
                JsonDecoder.decode(m.to_vec(), &mut batch, &Pushdown::default()),
                Decoded::Append
            );
        }
        let rb = batch.finish().unwrap();
        let ts = rb
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_793_363_696_789_012);
        assert_eq!(ts.value(1), 1_793_318_400_000_000);
        assert_eq!(ts.value(2), 1_793_363_696, "integer rides through in-unit");
        assert!(ts.is_null(3));
        let d = rb
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(d.value(0), 20756, "2026-10-30 in days since epoch");
        assert_eq!(d.value(1), 0);
        assert_eq!(d.value(2), 20756);
        assert!(d.is_null(3));
    }

    #[test]
    fn json_decode_root_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        assert_eq!(
            JsonDecoder.decode(
                br#"{"a":"x","n":3}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        assert_eq!(
            JsonDecoder.decode(b"not-json".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Bad(b"not-json".to_vec())
        );
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 1);
    }

    /// Regression lock for the typed extraction chain: JSON float/bool values
    /// used to go NULL after as_str/as_i64 both failed. Covers: float64
    /// column × float value, bool column × bool value, utf8 column × number
    /// (stringified), float64 column × integer value (widened via as_i64).
    #[test]
    fn json_decode_float_and_bool_values() {
        use datafusion::arrow::array::{BooleanArray, Float64Array, StringArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("ratio", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("note", DataType::Utf8, true),
            Field::new("exact", DataType::Float64, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        assert_eq!(
            JsonDecoder.decode(
                br#"{"ratio":0.5,"flag":true,"note":"ok","exact":2}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        assert_eq!(
            JsonDecoder.decode(
                br#"{"ratio":1.5,"flag":false,"note":9,"exact":3.25}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        let rb = batch.finish().unwrap();
        let ratio = rb
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            ratio.value(0),
            0.5,
            "float values must reach float64 columns (were NULL)"
        );
        assert_eq!(ratio.value(1), 1.5);
        let flag = rb
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(
            flag.value(0),
            "bool true must reach bool columns (was NULL)"
        );
        assert!(!flag.value(1));
        let note = rb.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(note.value(0), "ok");
        assert_eq!(
            note.value(1),
            "9",
            "numbers into utf8 columns stringify (was NULL)"
        );
        let exact = rb
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            exact.value(0),
            2.0,
            "integer values widen into float64 columns via as_i64"
        );
        assert_eq!(exact.value(1), 3.25);
    }

    /// Same regression family: OGG after.* float/bool columns.
    #[test]
    fn ogg_decode_after_float_and_bool() {
        use datafusion::arrow::array::{BooleanArray, Float64Array};
        let schema = Arc::new(Schema::new(vec![
            Field::new("op_type", DataType::Utf8, false),
            Field::new("ratio", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        assert_eq!(
            OggDecoder.decode(
                br#"{"table":"T","op_type":"I","ts":1,"after":{"ratio":0.75,"flag":true}}"#
                    .to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        let rb = batch.finish().unwrap();
        let ratio = rb
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(ratio.value(0), 0.75, "after.* floats (were NULL)");
        let flag = rb
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flag.value(0), "after.* bools (was NULL)");
    }

    /// Debezium c/r/u events decode their `after` payload into same-named
    /// columns across all typed builders; an update's `after` wins over
    /// `before` (before/op/ts_ms are envelope metadata, never decoded).
    #[test]
    fn debezium_decode_c_r_u_events() {
        use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let mut batch = ColBatch::new(schema, 8);
        // create
        assert_eq!(
            DebeziumJsonDecoder.decode(
                br#"{"before":null,"after":{"id":1,"name":"foo","price":9.5,"active":true},"op":"c","ts_ms":123}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        // snapshot read
        assert_eq!(
            DebeziumJsonDecoder.decode(
                br#"{"before":null,"after":{"id":2,"name":"bar","price":0.25,"active":false},"op":"r","ts_ms":124}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        // update: after (name=new) must win over before (name=old)
        assert_eq!(
            DebeziumJsonDecoder.decode(
                br#"{"before":{"id":3,"name":"old"},"after":{"id":3,"name":"new"},"op":"u","ts_ms":125}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 3);
        let id = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.value(0), 1);
        assert_eq!(id.value(2), 3);
        let name = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name.value(2), "new", "update after.* wins over before.*");
        let price = rb
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(price.value(0), 9.5);
        assert!(price.is_null(2), "absent after key → null");
        let active = rb
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active.value(0));
        assert!(!active.value(1));
        assert!(active.is_null(2));
    }

    /// Deletes (op=d) and `after`:null events (u out-of-shape / truncate `t`
    /// / message `m`) Skip without appending — v1 drops deletes and leaves
    /// the batch untouched. The tombstone fixtures are the REAL Kafka shapes:
    /// Debezium emits a null-valued record, which the conn layer hands
    /// through as an empty buffer (a bare JSON `null` text is tolerated but
    /// Kafka never delivers it for a tombstone). Events with NO `after` key
    /// are NOT here — they go Bad (see `debezium_malformed_goes_bad`).
    #[test]
    fn debezium_delete_and_tombstone_skip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut batch = ColBatch::new(schema, 8);
        let skips: Vec<&[u8]> = vec![
            // canonical delete: after=null, before carries the pre-image
            br#"{"before":{"id":1,"name":"foo"},"after":null,"op":"d","ts_ms":126}"#,
            // explicit after:null (no row image) — op u out-of-shape
            br#"{"before":{"id":1},"after":null,"op":"u"}"#,
            // truncate (op=t) and logical message (op=m): after=null by spec
            br#"{"before":null,"after":null,"op":"t","ts_ms":127}"#,
            br#"{"source":{},"after":null,"op":"m","ts_ms":128}"#,
            // tombstone, real shape: null-valued record -> EMPTY payload
            b"",
            // whitespace-only rides the tombstone rule
            b"\n",
            // bare JSON `null` text: tolerated (not a real tombstone shape)
            b"null",
        ];
        for s in skips {
            assert_eq!(
                DebeziumJsonDecoder.decode(s.to_vec(), &mut batch, &Pushdown::default()),
                Decoded::Skip,
                "{:?} must Skip",
                String::from_utf8_lossy(s)
            );
        }
        assert!(
            batch.is_empty(),
            "skipped events must leave the batch untouched"
        );
    }

    /// schemas.enable=true wrapper (Kafka Connect JsonConverter default):
    /// `{"schema":{…},"payload":{…}}` unwraps to the payload — a wrapped c
    /// event appends `after.*`, a wrapped delete skips. Without the unwrap
    /// every wrapped event silently skipped (no `after` at the root).
    #[test]
    fn debezium_schemas_enabled_wrapper_unwraps() {
        use datafusion::arrow::array::Int64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        assert_eq!(
            DebeziumJsonDecoder.decode(
                br#"{"schema":{"type":"struct","fields":[{"field":"id","type":"int64"}]},"payload":{"before":null,"after":{"id":5,"name":"wrapped"},"op":"c","ts_ms":9}}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        assert_eq!(
            DebeziumJsonDecoder.decode(
                br#"{"schema":{"type":"struct"},"payload":{"before":{"id":5},"after":null,"op":"d","ts_ms":10}}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Skip
        );
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 1);
        let id = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.value(0), 5);
    }

    /// Malformed JSON, a non-object root that is not a tombstone, an `after`
    /// that is a scalar/array, and an event object with NO `after` key
    /// (misrouted plain-row JSON, before-only shape, empty envelope, wrapper
    /// with a non-object payload) go Bad — the existing bad-row path
    /// (DLQ/fail) decides; the buffer rides along (T3 contract).
    #[test]
    fn debezium_malformed_goes_bad() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        for poisoned in [
            b"not-json".to_vec(),
            b"{\"before\":null,\"after\":{\"id\":1".to_vec(), // truncated
            b"\xff\xfe".to_vec(),                             // invalid UTF-8
            b"[1,2]".to_vec(),                                // not an event object
            b"42".to_vec(),                                   // scalar root
            br#"{"op":"c","after":7}"#.to_vec(),              // after not an object
            br#"{"op":"c","after":[1]}"#.to_vec(),            // after not an object
            b"{}".to_vec(),                                   // no after key at all
            br#"{"before":{"id":1},"op":"u"}"#.to_vec(),      // before-only, after absent
            br#"{"foo":"bar","id":1,"name":"x"}"#.to_vec(),   // misrouted plain-row JSON
            br#"{"schema":{},"payload":7}"#.to_vec(),         // wrapper payload not an object
        ] {
            let mut batch = ColBatch::new(schema.clone(), 4);
            match DebeziumJsonDecoder.decode(poisoned.clone(), &mut batch, &Pushdown::default()) {
                Decoded::Bad(buf) => assert_eq!(buf.len(), poisoned.len(), "buffer returned whole"),
                other => panic!(
                    "expected Bad for {:?}, got {:?}",
                    String::from_utf8_lossy(&poisoned),
                    other
                ),
            }
            assert!(batch.is_empty(), "Bad rows append nothing");
        }
    }

    /// Nested objects/arrays in field values and JSON-null field values
    /// decode as null columns (the JsonDecoder/OggDecoder family rule);
    /// sibling columns still decode and the row appends.
    #[test]
    fn debezium_nested_objects_and_null_fields() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("meta", DataType::Utf8, true),
            Field::new("tags", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        assert_eq!(
            DebeziumJsonDecoder.decode(
                br#"{"before":null,"after":{"id":3,"meta":{"k":"v"},"tags":[1,2],"name":null},"op":"c","ts_ms":128}"#.to_vec(),
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 1);
        let id = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.value(0), 3, "sibling columns still decode");
        let meta = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(meta.is_null(0), "nested object field → null");
        let tags = rb.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(tags.is_null(0), "array field → null");
        let name = rb.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(name.is_null(0), "JSON null field value → null");
    }

    /// Mixed batch — the pipeline shape: c/u append, d and a real tombstone
    /// (empty payload, the conn-layer shape of a null-valued record) skip,
    /// one malformed row goes Bad; appended + skipped counts must net out.
    #[test]
    fn debezium_mixed_batch_counts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut batch = ColBatch::new(schema, 8);
        let mut appended = 0;
        let mut skipped = 0;
        let mut bad = 0;
        for m in [
            &br#"{"before":null,"after":{"id":1,"name":"a"},"op":"c","ts_ms":1}"#[..],
            &br#"{"before":null,"after":{"id":2,"name":"b"},"op":"r","ts_ms":2}"#[..],
            &br#"{"before":{"id":1},"after":null,"op":"d","ts_ms":3}"#[..],
            &b""[..], // tombstone: null-valued record -> empty payload
            b"{garbage",
        ] {
            match DebeziumJsonDecoder.decode(m.to_vec(), &mut batch, &Pushdown::default()) {
                Decoded::Append => appended += 1,
                Decoded::Skip => skipped += 1,
                Decoded::Bad(_) => bad += 1,
            }
        }
        assert_eq!((appended, skipped, bad), (2, 2, 1));
        assert_eq!(batch.len(), 2, "only appended rows sit in the batch");
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 2);
        assert!(
            rb.column(0).len() == 2 && rb.column(1).len() == 2,
            "column lengths stay equal"
        );
    }

    #[test]
    fn csv_decode_typed_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
        ]));
        let mut batch = ColBatch::new(schema, 8);
        assert_eq!(
            CsvDecoder.decode(b"w1,10,0.5,true".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Append
        );
        // Bad numeric columns degrade to null (the row is not dropped)
        assert_eq!(
            CsvDecoder.decode(b"w2,xx,,false".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Append
        );
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 2);
        let qty = rb
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(qty.value(0), 10);
        assert!(qty.is_null(1));
        let ok = rb
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        assert!(ok.value(0));
    }

    /// T4 RFC4180 compliance: the csv branch used to be a bare `split(',')`
    /// — quoted commas, doubled quotes and in-field newlines were all cut
    /// into garbage fields. Every case here decodes the quoted payload
    /// intact.
    #[test]
    fn csv_decode_rfc4180_quoted_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
        ]));
        let cases: Vec<(&[u8], &str, i64)> = vec![
            // quoted comma: the splitter produced ["a", "b\""] garbage
            (b"\"a,b\",10", "a,b", 10),
            // doubled quotes escape (RFC4180 §2.7): "he said ""hi"""
            (b"\"he said \"\"hi\"\"\",7", "he said \"hi\"", 7),
            // in-field newline inside quotes (a whole RFC4180 record)
            (b"\"line1\nline2\",3", "line1\nline2", 3),
            // quotes + comma + emoji together
            (
                "\"emoji 🎉, \"\"quoted\"\"\",1".as_bytes(),
                "emoji 🎉, \"quoted\"",
                1,
            ),
            // unquoted field keeps an unpaired apostrophe/quote
            (b"5'9\",2", "5'9\"", 2),
            // quoted empty field ≠ unquoted empty: both utf8-null here
            (b"\"\",5", "", 5),
        ];
        let mut batch = ColBatch::new(schema, 8);
        for (raw, _want_name, _want_qty) in &cases {
            assert_eq!(
                CsvDecoder.decode(raw.to_vec(), &mut batch, &Pushdown::default()),
                Decoded::Append
            );
        }
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), cases.len());
        let names = rb
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let qtys = rb
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        for (i, (_raw, want_name, want_qty)) in cases.iter().enumerate() {
            if want_name.is_empty() {
                assert!(names.is_null(i), "row {i}: quoted empty → null");
            } else {
                assert_eq!(names.value(i), *want_name, "row {i} field value");
            }
            assert_eq!(qtys.value(i), *want_qty, "row {i} qty");
        }
    }

    /// RFC4180 edges around emptiness and broken records: blank input skips,
    /// invalid UTF-8 goes Bad (bad-row policy decides upstream), and ragged
    /// records keep the positional mapping (missing trailing field → null).
    #[test]
    fn csv_decode_blank_skip_utf8_bad_and_ragged() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, true),
        ]));
        let mut batch = ColBatch::new(schema, 8);
        assert_eq!(
            CsvDecoder.decode(b"".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Skip
        );
        assert_eq!(
            CsvDecoder.decode(b"\n".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Skip
        );
        assert_eq!(
            CsvDecoder.decode(b"   ".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Skip
        );
        assert_eq!(
            CsvDecoder.decode(b"\xff\xfe".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Bad(b"\xff\xfe".to_vec())
        );
        // ragged: one field only → qty column null, row kept
        assert_eq!(
            CsvDecoder.decode(b"solo".to_vec(), &mut batch, &Pushdown::default()),
            Decoded::Append
        );
        let rb = batch.finish().unwrap();
        assert_eq!(rb.num_rows(), 1);
        assert!(rb
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn parquet_decode_roundtrip_with_cast() -> anyhow::Result<()> {
        use datafusion::arrow::array::{
            BooleanArray, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
        };
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::parquet::arrow::ArrowWriter;

        // fixture: all five types (incl. Timestamp→utf8, the dual of the
        // console's approximate type mapping)
        let src_schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let rb = RecordBatch::try_new(
            src_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Float64Array::from(vec![0.5, 1.5])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    1726000000000,
                    1726000000001,
                ])),
            ],
        )?;
        let dir = std::env::temp_dir().join(format!("tailrace-decode-pq-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let pq = dir.join("fixture.parquet");
        {
            let mut w = ArrowWriter::try_new(std::fs::File::create(&pq)?, src_schema, None)?;
            w.write(&rb)?;
            w.close()?;
        }
        let raw = std::fs::read(&pq)?;

        // target schema: ts approximately utf8 (matching the inference mapping)
        let target = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("ts", DataType::Utf8, true),
        ]));
        let mut batch = ColBatch::new(target, 8);
        assert_eq!(
            ParquetDecoder.decode(raw, &mut batch, &Pushdown::default()),
            Decoded::Append
        );
        let out = batch.finish().unwrap();
        assert_eq!(out.num_rows(), 2);
        let oid = out
            .column_by_name("oid")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(oid.value(1), 2);
        let ts = out
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            ts.value(0).contains("202"),
            "Timestamp cast to utf8: {}",
            ts.value(0)
        );
        assert!(out
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap()
            .is_null(1));
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
    /// T3: the owned-buffer signature hands the buffer back on Bad (the
    /// DLQ/fail-sample path depends on it). Plain malformed rows come back
    /// byte-identical; the doc on `Decoded::Bad` covers the escape-rewrite
    /// caveat measured on poisoned rows carrying escapes.
    #[test]
    fn bad_rows_hand_the_buffer_back() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        for poisoned in [
            b"not-json".to_vec(),
            b"{\"a\":\"x".to_vec(),
            b"{\"a\":1,\"b\":".to_vec(),
            b"\xff\xfe".to_vec(),
        ] {
            let mut batch = ColBatch::new(schema.clone(), 4);
            match JsonDecoder.decode(poisoned.clone(), &mut batch, &Pushdown::default()) {
                Decoded::Bad(buf) => {
                    assert_eq!(buf.len(), poisoned.len(), "buffer returned whole");
                    assert_eq!(buf, poisoned, "plain malformed rows stay byte-identical");
                }
                other => panic!("expected Bad, got {other:?}"),
            }
            // OGG shape without `after` also hands the buffer back
            let mut batch = ColBatch::new(schema.clone(), 4);
            match OggDecoder.decode(
                b"{\"table\":\"T\"}".to_vec(),
                &mut batch,
                &Pushdown::default(),
            ) {
                Decoded::Bad(buf) => assert_eq!(buf, b"{\"table\":\"T\"}".to_vec()),
                other => panic!("expected Bad, got {other:?}"),
            }
        }
    }

    #[test]
    fn coerce_keeps_column_lengths_equal() {
        // Regression lock: numbers into utf8 columns / numeric strings into
        // number columns used to silently drop the column, leaving unequal
        // lengths at finish. Now every value is appended by the column's real
        // type — equal column lengths are an invariant.
        use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let mut batch = ColBatch::new(schema.clone(), 8);
        // row 1: number into utf8 / numeric string into int64 / non-numeric into float64 (null)
        append_i64(batch.column_mut(0), 42);
        append_str(batch.column_mut(1), Some("7"));
        append_str(batch.column_mut(2), Some("not-a-num"));
        // row 2: null family
        append_i64_opt(batch.column_mut(0), None);
        append_str(batch.column_mut(1), None);
        append_str(batch.column_mut(2), Some("1.5"));
        let rb = batch
            .finish()
            .expect("equal column lengths — coercion always completes a batch");
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.column(0).len(), 2);
        let sa = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(sa.value(0), "42");
        assert!(sa.is_null(1));
        let ia = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ia.value(0), 7);
        let fa = rb
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(fa.is_null(0));
        assert_eq!(fa.value(1), 1.5);
    }
}
