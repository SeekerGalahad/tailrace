//! tailrace-decode: Decoder trait (bytes → Arrow columns, written straight
//! into column builders — no row-shaped intermediate) plus ogg/json/csv/parquet
//! decoders.
//!
//! ogg (Oracle GoldenGate CDC envelopes) is a generic adapter name: the payload
//! carries `after` + three envelope columns (op_type/ogg_table/ogg_ts).
//! Cross-sample typing is carried by the target schema (this crate only writes
//! declared schema columns; inference belongs to the console discover step).
//! parquet brings its own schema: the whole file is one batch, column-cast
//! into the target via `ColBatch::append_batch`.

use datafusion::arrow::array::{
    ArrayBuilder, BooleanBuilder, Date64Builder, Float64Builder, Int64Builder, StringBuilder,
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
                other => unimplemented!("decode column type {other:?}"),
            };
            builders.push(b);
        }
        Self { schema, builders }
    }

    pub fn column_mut(&mut self, i: usize) -> &mut dyn ArrayBuilder {
        self.builders[i].as_mut()
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

/// Append per target column type (post-cast only five types:
/// utf8/int64/float64/bool/date64).
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
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        if let Some(db) = b.as_any_mut().downcast_mut::<Date64Builder>() {
            for i in 0..a.len() {
                db.append_option(if a.is_null(i) { None } else { Some(a.value(i)) });
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

/// Decode outcome: Append=column-filled / Skip=dropped by pushdown early
/// filter / Bad=parse failure.
#[derive(PartialEq, Debug)]
pub enum Decoded {
    Append,
    Skip,
    Bad,
}

/// Decoder: bytes → straight into the target schema columns. pushdown = the
/// hook for compile-time constant-predicate early filtering.
pub trait Decoder: Send {
    fn decode(&self, raw: &[u8], batch: &mut ColBatch, pushdown: &Pushdown) -> Decoded;
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

/// JSON decoder: root object keys write into same-named columns (schema
/// column names match JSON keys).
pub struct JsonDecoder;

impl Decoder for JsonDecoder {
    fn decode(&self, raw: &[u8], batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        let mut scratch = raw.to_vec();
        let dom = match simd_json::to_borrowed_value(scratch.as_mut_slice()) {
            Ok(v) => v,
            Err(_) => return Decoded::Bad,
        };
        // Typed extraction chain: as_str → as_i64 → as_f64 → as_bool → null.
        // (Floats and bools used to fall through to NULL when as_str/as_i64
        // both failed — whole float64/bool columns silently lost their values.)
        for (i, f) in batch_fields(batch).iter().enumerate() {
            match dom.get(f.as_str()).and_then(|v| v.as_str()) {
                Some(s) => append_str(batch.column_mut(i), Some(s)),
                None => {
                    let b = batch.column_mut(i);
                    if let Some(iv) = dom.get(f.as_str()).and_then(|v| v.as_i64()) {
                        append_i64(b, iv);
                    } else if let Some(fv) = dom.get(f.as_str()).and_then(|v| v.as_f64()) {
                        append_f64(b, fv);
                    } else if let Some(bv) = dom.get(f.as_str()).and_then(|v| v.as_bool()) {
                        append_bool(b, bv);
                    } else {
                        append_str(b, None);
                    }
                }
            }
        }
        Decoded::Append
    }
}

// ==================== OGG (after + three envelope columns) ====================

/// OGG decoder: envelope columns first (op_type/ogg_table/ogg_ts), after.*
/// columns after — the column order matches the console schema inference.
pub struct OggDecoder;

impl Decoder for OggDecoder {
    fn decode(&self, raw: &[u8], batch: &mut ColBatch, pushdown: &Pushdown) -> Decoded {
        let mut scratch = raw.to_vec();
        let dom = match simd_json::to_borrowed_value(scratch.as_mut_slice()) {
            Ok(v) => v,
            Err(_) => return Decoded::Bad,
        };
        let after = match dom.get("after") {
            Some(v) => v,
            None => return Decoded::Bad, // an OGG shape must have `after`
        };
        let fields = batch_fields(batch);
        // Pushdown early filter: op_type outside the whitelist → dropped with
        // zero column copies.
        if !pushdown.eq_whitelist.is_empty() {
            if let Some(allowed) = pushdown.eq_whitelist.get("op_type") {
                let op = dom.get("op_type").and_then(|v| v.as_str()).unwrap_or("");
                if !allowed.iter().any(|a| a == op) {
                    return Decoded::Skip;
                }
            }
        }
        for (i, f) in fields.iter().enumerate() {
            match f.as_str() {
                "op_type" => append_str(
                    batch.column_mut(i),
                    dom.get("op_type").and_then(|v| v.as_str()),
                ),
                "ogg_table" => append_str(
                    batch.column_mut(i),
                    dom.get("table").and_then(|v| v.as_str()),
                ),
                "ogg_ts" => append_i64(
                    batch.column_mut(i),
                    dom.get("ts").and_then(|v| v.as_i64()).unwrap_or(0),
                ),
                name => match after.get(name).and_then(|v| v.as_str()) {
                    Some(s) => append_str(batch.column_mut(i), Some(s)),
                    None => {
                        // Same typed chain as JsonDecoder: after.* floats and
                        // bools must not fall through to NULL.
                        let b = batch.column_mut(i);
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
        Decoded::Append
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
    fn decode(&self, raw: &[u8], batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        let line = match std::str::from_utf8(raw) {
            Ok(s) => s.trim_end(),
            Err(_) => return Decoded::Bad,
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
            Err(_) => return Decoded::Bad,
        }
        let typed = batch_fields_typed(batch);
        for (i, (_name, dt)) in typed.iter().enumerate() {
            let v = rec.get(i).map(str::trim).unwrap_or("");
            let b = batch.column_mut(i);
            match dt {
                DataType::Int64 => match v.parse::<i64>() {
                    Ok(n) => append_i64(b, n),
                    Err(_) => append_i64_opt(b, None),
                },
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
    fn decode(&self, raw: &[u8], batch: &mut ColBatch, _pushdown: &Pushdown) -> Decoded {
        use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let bytes = bytes::Bytes::from(raw.to_vec());
        let builder = match ParquetRecordBatchReaderBuilder::try_new(bytes) {
            Ok(b) => b,
            Err(_) => return Decoded::Bad,
        };
        let reader = match builder.build() {
            Ok(r) => r,
            Err(_) => return Decoded::Bad,
        };
        for rb in reader {
            match rb {
                Ok(rb) => {
                    if batch.append_batch(&rb).is_err() {
                        return Decoded::Bad;
                    }
                }
                Err(_) => return Decoded::Bad,
            }
        }
        Decoded::Append
    }
}

// ColBatch field-name snapshot (avoids borrow conflicts: clone the names
// first, then write column by column).
fn batch_fields(batch: &ColBatch) -> Vec<String> {
    batch_fields_of(batch)
}

fn batch_fields_of(batch: &ColBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// Field name + type snapshot (CsvDecoder parses by column type).
fn batch_fields_typed(batch: &ColBatch) -> Vec<(String, DataType)> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
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
            if ogg.decode(m, &mut batch, &pushdown) == Decoded::Append {
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

    #[test]
    fn json_decode_root_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let mut batch = ColBatch::new(schema, 4);
        assert_eq!(
            JsonDecoder.decode(br#"{"a":"x","n":3}"#, &mut batch, &Pushdown::default()),
            Decoded::Append
        );
        assert_eq!(
            JsonDecoder.decode(b"not-json", &mut batch, &Pushdown::default()),
            Decoded::Bad
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
                br#"{"ratio":0.5,"flag":true,"note":"ok","exact":2}"#,
                &mut batch,
                &Pushdown::default()
            ),
            Decoded::Append
        );
        assert_eq!(
            JsonDecoder.decode(
                br#"{"ratio":1.5,"flag":false,"note":9,"exact":3.25}"#,
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
                br#"{"table":"T","op_type":"I","ts":1,"after":{"ratio":0.75,"flag":true}}"#,
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
            CsvDecoder.decode(b"w1,10,0.5,true", &mut batch, &Pushdown::default()),
            Decoded::Append
        );
        // Bad numeric columns degrade to null (the row is not dropped)
        assert_eq!(
            CsvDecoder.decode(b"w2,xx,,false", &mut batch, &Pushdown::default()),
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
                CsvDecoder.decode(raw, &mut batch, &Pushdown::default()),
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
            CsvDecoder.decode(b"", &mut batch, &Pushdown::default()),
            Decoded::Skip
        );
        assert_eq!(
            CsvDecoder.decode(b"\n", &mut batch, &Pushdown::default()),
            Decoded::Skip
        );
        assert_eq!(
            CsvDecoder.decode(b"   ", &mut batch, &Pushdown::default()),
            Decoded::Skip
        );
        assert_eq!(
            CsvDecoder.decode(b"\xff\xfe", &mut batch, &Pushdown::default()),
            Decoded::Bad
        );
        // ragged: one field only → qty column null, row kept
        assert_eq!(
            CsvDecoder.decode(b"solo", &mut batch, &Pushdown::default()),
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
            ParquetDecoder.decode(&raw, &mut batch, &Pushdown::default()),
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
