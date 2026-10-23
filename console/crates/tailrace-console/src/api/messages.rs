//! zh 单轨文案字典 + §4.0 信封构造（roadmap §九 T34：T30 登记的 v1.x 项兑现）。
//!
//! console API 的用户可见文案收敛为 message_key 查表——zh 是唯一轨道
//! （en 留空轨：key 已经以 `message_key` 随 wire 携带（本模块的信封构造，
//! T30 通道兑现），未来翻译轨按 key 展开即可，本模块不再加第二张表）。
//! 模板占位 `{0}`..`{9}`，由 [`t`] 位置插值；渲染结果与收敛前的直写输出
//! 逐字节一致（既有测试与前端提示不漂移）。
//!
//! 信封：`{"error":{code,message,message_key[,refs]}}`——与 console-auth
//! `ApiError::into_response` 的 §4.0 形态逐字节同形（多出 message_key 一键，
//! additive；由模块测试钉死字节）。走本模块的响应不再经过 ApiError。
//!
//! 豁免边界（不进字典的中文文案类，T34 卡内登记）：
//! ① audit 台账文案——`AuditEntry{action,entity_type,entity_id,detail_json}`
//!    均为机器 JSON（name/version/sha256/引用数组），无中文文案；
//! ② 非格式化直写的字面量文案（console-auth 5 个构造器字面量等）——44 处
//!    锚点只覆盖格式化直写类；
//! ③ 数据源注册表 UI 元数据标签（label/credential_label）；
//! ④ main.rs 启动横幅 eprintln（stderr，非 API 面）；
//! ⑤ spa.rs/openapi.rs 的说明性文案；
//! ⑥ console-proxy crate 内文案（本卡文件域外，含 Display 字符串）；
//! ⑦ `src/compile.rs`（44 锚点内含 1 处，但不在本卡文件域
//!    `console/crates/tailrace-console/src/api/*` 内——域外不动，留后续卡；
//!    完整性扫描按路径豁免并在此登记）。
//! 完整性由 [`tests::no_chinese_format_direct_writes_outside_dictionary`]
//! 钉住：src/ 内（字典模块与 compile.rs 之外）格式化直写 + 中文的行必须
//! 为零。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Map, Value};

/// key → zh 模板（唯一文案定义点；占位 `{0}`..`{9}`）。
fn template(key: &str) -> Option<&'static str> {
    Some(match key {
        // ---- auth ----
        "err.auth.bad_login_request" => "非法登录请求: {0}",
        "err.auth.locked" => "尝试过于频繁，请 {0} 秒后再试",
        // ---- 通用 ----
        "err.not_implemented" => "{0} 尚未实现（{1}）；本端点按计划推迟到后续阶段交付",
        "err.bad_body" => "非法请求体: {0}",
        // ---- artifacts ----
        "err.artifact.multipart_read_failed" => "multipart 读取失败: {0}",
        "err.artifact.field_read_failed" => "字段 {0} 读取失败",
        "err.artifact.duplicate" => "制品 {0} {1} 已存在",
        "err.artifact.not_found" => "制品 #{0} 不存在",
        "err.artifact.in_use" => "制品被节点引用: {0}",
        // ---- nodes ----
        "err.node.not_found" => "节点 #{0} 不存在",
        "err.node.bad_register_body" => "非法注册请求: {0}",
        "err.node.bad_heartbeat_body" => "非法心跳请求: {0}",
        "err.node.probe_failed" => "节点探活失败：{0}/healthz 未返回 200",
        "err.node.in_use" => "节点仍被管道指派: {0}",
        // ---- pipelines ----
        "err.pipeline.not_found" => "管道 #{0} 不存在",
        "err.pipeline.table_objects_missing" => "表对象未登记（先在数据源页登记）: {0}",
        "err.pipeline.statement_table_missing" => {
            "语句 {0} 的{1}表对象未登记: {2}（先在数据源页登记库表）"
        }
        "err.pipeline.node_no_api_url" => "节点 {0} 未上报 api_url（等待首次心跳）",
        "err.pipeline.engine_assembly_failed" => "引擎装配失败: {0}",
        "err.pipeline.undeployed" => "管道 #{0} 未部署（无指派节点）",
        // 引擎镜像形态（{"ok":false,"error":…} 透传语义，非 §4.0 信封）
        "err.pipeline.undeployed_plain" => "pipeline #{0} 未部署（无指派节点）",
        "err.pipeline.assigned_node_missing" => "指派节点 #{0} 不存在",
        // ---- datasources ----
        "err.datasource.not_found" => "数据源 #{0} 不存在",
        "err.datasource.unknown_type" => "未知数据源类型 {0}",
        "err.datasource.unknown_type_registry" => {
            "未知数据源类型 {0}（注册表：kafka/doris/file/mysql/postgresql）"
        }
        "err.datasource.missing_required_fields" => "connJson 缺少必填字段: {0}",
        "err.datasource.in_use" => "数据源仍被引用（管道 {0} 个，表对象 {1} 个）",
        "err.datasource.jdbc_url_unparsable" => "jdbcUrl 无法解析（需 host:port）: {0}",
        "err.datasource.jdbc_mysql_unparsable" => {
            "jdbcUrl 无法解析（需 jdbc:mysql://host:port/db）: {0}"
        }
        "err.datasource.jdbc_pg_unparsable" => {
            "jdbcUrl 无法解析（需 jdbc:postgresql://host:port/db）: {0}"
        }
        "err.datasource.kafka_format_unsupported" => {
            "kafka 样本推断不支持格式 {0}（ogg/json/jsonl；csv/parquet 请手工登记列）"
        }
        // ---- 探测报告（probe outcome，非 §4.0 信封）----
        "probe.jdbc.timeout" => "{0}: 超时 (>3s)",
        "probe.jdbc.reachable" => "jdbc server 可达（{0}）",
        "probe.jdbc.unreachable" => "jdbc server 不可达: {0}",
        // ---- 表对象 ----
        "err.table_object.not_found" => "表对象 #{0} 不存在",
        "err.table_object.in_use" => "表对象被已发布管道引用: {0}",
        _ => return None,
    })
}

/// 查表 + 位置插值。未知 key 渲染为 key 本身（可 grep、可暴露缺字典；
/// 完整性测试保证调用点不会命中该分支）。
pub fn t(key: &str, args: &[&str]) -> String {
    let mut out = template(key).unwrap_or(key).to_string();
    for (i, a) in args.iter().enumerate() {
        out = out.replace(&format!("{{{i}}}"), a);
    }
    out
}

/// §4.0 信封体（纯函数，便于同步断言字节）：`message_key` 与 `message`
/// 同行（T30 通道兑现），`refs` 可选。
fn envelope(code: &str, key: &str, args: &[&str], refs: Option<Map<String, Value>>) -> Value {
    let mut error = serde_json::json!({ "code": code, "message": t(key, args) });
    error["message_key"] = Value::String(key.to_string());
    if let Some(refs) = refs {
        error["refs"] = Value::Object(refs);
    }
    serde_json::json!({ "error": error })
}

/// §4.0 信封响应（查表文案 + `message_key` 携带 + HTTP 状态）。
pub fn respond(status: StatusCode, code: &str, key: &str, args: &[&str]) -> Response {
    (status, Json(envelope(code, key, args, None))).into_response()
}

/// §4.0 信封响应 + `refs` 合并（IN_USE 删除守卫等结构化载荷）。
pub fn respond_with_refs(
    status: StatusCode,
    code: &str,
    key: &str,
    args: &[&str],
    refs: Map<String, Value>,
) -> Response {
    (status, Json(envelope(code, key, args, Some(refs)))).into_response()
}

/// 400 INVALID_ARGUMENT 信封。
pub fn bad_request(key: &str, args: &[&str]) -> Response {
    respond(
        StatusCode::BAD_REQUEST,
        console_auth::error::code::INVALID_ARGUMENT,
        key,
        args,
    )
}

/// 404 NOT_FOUND 信封。
pub fn not_found(key: &str, args: &[&str]) -> Response {
    respond(
        StatusCode::NOT_FOUND,
        console_auth::error::code::NOT_FOUND,
        key,
        args,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 插值渲染与收敛前的直写输出逐字节一致（抽查带 0/1/2 参模板，
    /// 对照串为字面量）。
    #[test]
    fn interpolation_is_byte_identical_to_legacy_direct_writes() {
        assert_eq!(
            t("err.auth.locked", &["30"]),
            "尝试过于频繁，请 30 秒后再试"
        );
        assert_eq!(
            t("err.artifact.duplicate", &["loader", "1.2.0"]),
            "制品 loader 1.2.0 已存在"
        );
        assert_eq!(
            t("err.datasource.jdbc_url_unparsable", &["jdbc:x://bad"]),
            "jdbcUrl 无法解析（需 host:port）: jdbc:x://bad"
        );
        // 集成测试锚点（nodes_admin.rs 断言 contains "八步"）：参数原样透传
        assert!(t("err.not_implemented", &["SSH 部署链八步", "§4.4"]).contains("八步"));
    }

    /// 未知 key 渲染为 key 本身（可 grep 的缺字典信号，不给空串）。
    #[test]
    fn unknown_key_falls_back_to_key() {
        assert_eq!(t("err.no_such_key", &[]), "err.no_such_key");
    }

    /// 字典自洽：模板只含 {数字} 占位，且插值后无残留花括号。
    #[test]
    fn templates_use_only_positional_placeholders() {
        let keys: Vec<&str> = KEY_LIST.to_vec();
        assert!(!keys.is_empty());
        for key in keys {
            let tpl = template(key).expect("listed key resolves");
            let mut rest = tpl;
            while let Some(open) = rest.find('{') {
                let close = rest[open..].find('}').expect("balanced braces") + open;
                let inner = &rest[open + 1..close];
                assert!(
                    inner.chars().all(|c| c.is_ascii_digit()) && !inner.is_empty(),
                    "{key}: non-positional placeholder {{{inner}}}"
                );
                rest = &rest[close + 1..];
            }
            let filled = t(key, &["x", "x", "x"]);
            assert!(
                !filled.contains('{') && !filled.contains('}'),
                "{key}: {filled}"
            );
        }
    }

    /// §4.0 信封字节（T30 通道兑现后的 wire 形态）：code → message →
    /// message_key → refs 的键序与 ApiError 既有信封同形，仅多 message_key
    /// 一键（additive）。
    #[test]
    fn envelope_bytes_match_apierror_shape_plus_message_key() {
        assert_eq!(
            envelope("NOT_FOUND", "err.pipeline.not_found", &["42"], None).to_string(),
            r#"{"error":{"code":"NOT_FOUND","message":"管道 #42 不存在","message_key":"err.pipeline.not_found"}}"#
        );
        let mut refs = Map::new();
        refs.insert("pipelines".into(), serde_json::json!([7]));
        assert_eq!(
            envelope("IN_USE", "err.node.in_use", &["[7]"], Some(refs)).to_string(),
            r#"{"error":{"code":"IN_USE","message":"节点仍被管道指派: [7]","message_key":"err.node.in_use","refs":{"pipelines":[7]}}}"#
        );
    }

    /// 字典完整性（卡内 ③）：src/ 全树扫描——豁免路径之外不允许出现
    /// 「格式化宏调用 + 中文」的直写（收敛后为零漏网；新增文案必须进
    /// 字典）。**多行感知**：`format!(` 与中文串分行写的直写同样抓
    /// （对每个 format! 调用取括号平衡的完整切片再判 CJK——44 行级
    /// 锚点本身漏掉的多行直写即由此抓出）。扫描类 = T34 锚点类（格式化
    /// 直写）；字面量/标签/横幅等豁免边界见模块文档。
    #[test]
    fn no_chinese_format_direct_writes_outside_dictionary() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        // 豁免路径：字典模块本身（模板为字面量）与 compile.rs（锚点内
        // 含 1 处但文件不在本卡文件域——域外不动项，卡内登记）
        let exempt: &[&str] = &["api/messages.rs", "compile.rs"];
        let mut offenders: Vec<String> = Vec::new();
        let mut stack = vec![src.clone()];
        while let Some(dir) = stack.pop() {
            let entries = std::fs::read_dir(&dir).expect("read src dir");
            for entry in entries {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let rel = path.strip_prefix(&src).expect("under src").to_path_buf();
                let rel_str = rel.to_string_lossy().to_string();
                if exempt.contains(&rel_str.as_str()) {
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("read source");
                for off in format_call_spans(&text) {
                    let call = &text[off.clone()];
                    if contains_cjk(call) {
                        let lineno = text[..off.start].matches('\n').count() + 1;
                        offenders.push(format!("{rel_str}:{lineno},"));
                    }
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "中文格式化直写漏网（须收敛进 api/messages.rs 字典）:\n{}",
            offenders.join("\n")
        );
    }

    /// 每个格式化宏调用的字符区间（起于 `format!(`，止于配对闭括号）。
    fn format_call_spans(text: &str) -> Vec<std::ops::Range<usize>> {
        let bytes = text.as_bytes();
        let mut spans = Vec::new();
        let mut i = 0;
        while let Some(rel) = text[i..].find("format!(") {
            let start = i + rel;
            let mut depth = 0usize;
            let mut j = start + "format!".len();
            let mut in_str = false;
            let mut escaped = false;
            while j < bytes.len() {
                let c = bytes[j] as char;
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_str = !in_str;
                } else if !in_str {
                    if c == '(' {
                        depth += 1;
                    } else if c == ')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                j += 1;
            }
            spans.push(start..(j + 1).min(bytes.len()));
            i = j + 1;
        }
        spans
    }

    /// CJK 判定：汉字 + CJK 标点/全角符号（文案必然落在这些区段）。
    fn contains_cjk(s: &str) -> bool {
        s.chars().any(|c| {
            matches!(c, '\u{4E00}'..='\u{9FFF}' | '\u{3000}'..='\u{303F}' | '\u{FF00}'..='\u{FFEF}')
        })
    }

    /// key 清单（占位完整性测试与字典同步的唯一登记点）。
    const KEY_LIST: &[&str] = &[
        "err.auth.bad_login_request",
        "err.auth.locked",
        "err.not_implemented",
        "err.bad_body",
        "err.artifact.multipart_read_failed",
        "err.artifact.field_read_failed",
        "err.artifact.duplicate",
        "err.artifact.not_found",
        "err.artifact.in_use",
        "err.node.not_found",
        "err.node.bad_register_body",
        "err.node.bad_heartbeat_body",
        "err.node.probe_failed",
        "err.node.in_use",
        "err.pipeline.not_found",
        "err.pipeline.table_objects_missing",
        "err.pipeline.statement_table_missing",
        "err.pipeline.node_no_api_url",
        "err.pipeline.engine_assembly_failed",
        "err.pipeline.undeployed",
        "err.pipeline.undeployed_plain",
        "err.pipeline.assigned_node_missing",
        "err.datasource.not_found",
        "err.datasource.unknown_type",
        "err.datasource.unknown_type_registry",
        "err.datasource.missing_required_fields",
        "err.datasource.in_use",
        "err.datasource.jdbc_url_unparsable",
        "err.datasource.jdbc_mysql_unparsable",
        "err.datasource.jdbc_pg_unparsable",
        "err.datasource.kafka_format_unsupported",
        "probe.jdbc.timeout",
        "probe.jdbc.reachable",
        "probe.jdbc.unreachable",
        "err.table_object.not_found",
        "err.table_object.in_use",
    ];
}
