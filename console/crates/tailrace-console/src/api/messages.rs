//! zh 单轨文案字典 + §4.0 信封构造（roadmap §九 T34：T30 登记的 v1.x 项兑现）。
//!
//! console API 的**格式化直写类**用户可见文案收敛为 message_key 查表——
//! zh 是唯一轨道（en 留空轨：key 已经以 `message_key` 随 wire 携带
//! （本模块的信封构造，T30 通道兑现），未来翻译轨按 key 展开即可，本
//! 模块不再加第二张表）。走 ApiError 通道的中文字面量文案**不在本次
//! 收敛范围**（豁免②：残留清单如实登记并由测试机器钉住，翻译轨到时
//! 按清单二次展开）。
//! 模板占位 `{0}`..`{9}`，由 [`t`] 位置插值；渲染结果与收敛前的直写输出
//! 逐字节一致（既有测试与前端提示不漂移）。
//!
//! 规模口径（T39 ③ 勘正，2026-10-23 实测；T35 增补；T-D1 dim join +6 key；
//! T-A1 建表闭环 +10 key；T-A2 回填 +9 key；T-A7 部分列更新 +9 key，
//! 2026-10-28 实测；R7 评审修复轮 +1 key——回填 accept 重跑部分列发布门；
//! LOW-5 R8 +2 key——publish 活表核实 2；T-E2 告警 +6 key——规则 CRUD 门禁，
//! 2026-10-31 实测）：字典
//! **82 key**（[`tests::KEY_LIST`] 与 `template` 同步，长度由
//! [`tests::templates_use_only_positional_placeholders`]
//! 钉住）/ **120 调用点**（`grep -rn 'messages::' src/ --include='*.rs'`
//! 排除本模块，2026-10-31 T-E2 改后实测——改前 109；+11：alerts.rs 规则
//! CRUD 的 400/404/409 信封与 bad_body）；
//! T35 部署链 +3 key/+7 站点；T-D1 +6 站点——发布门禁 3 + 维表登记 3；
//! T-A1 +14 站点——draft/execute/evolve 三端点的 400/404/422/409/502 信封全部走字典，
//! 改前实测 66，此前登记的 63 为 T-D1 后未同步的旧值；T-A2 回填 +11 站点，
//! 2026-10-28 实测——backfill 请求门禁 9 + jobs 404 1 + 未发布 400 1；
//! T-A7 +14 站点——发布门禁 6 + 部署侧互斥 2 + evolve 侧互斥 1 + keyModel
//! 登记/PUT 5，改前实测 91；R7 评审修复轮 +1 站点——backfill 部分列重门 422）。
//! T34 首提交 3cf738b 的「37 key/51 调用点」为双 off-by-one
//! 误记，ffb1d55 勘正、本次落为模块事实。
//!
//! 信封：`{"error":{code,message,message_key[,refs]}}`——与 console-auth
//! `ApiError::into_response` 的 §4.0 形态逐字节同形（多出 message_key 一键，
//! additive；由模块测试钉死字节）。走本模块的响应不再经过 ApiError。
//!
//! 豁免边界（不进字典的中文文案类，T34 卡内登记）：
//! ① audit 台账文案——`AuditEntry{action,entity_type,entity_id,detail_json}`
//!    均为机器 JSON（name/version/sha256/引用数组），无中文文案；
//! ② ApiError 通道的中文字面量错误响应（非格式化直写，44 处锚点不覆盖）
//!    ——api/ 域内残留 34 站点：datasources 11 / artifacts 10 / pipelines 7 /
//!    nodes_admin 3 / auth 2 / nodes 1（bad_request/not_found 单字面量 19、
//!    internal+anyhow! 格式化 4、new 三参字面量 11；console-auth crate 内
//!    5 个构造器字面量在域外另计）——均无 message_key，是 T34 收敛度的
//!    如实口径；清单由 [`tests::api_error_chinese_literals_match_pinned_registry`]
//!    按文件×文案多重集机器钉住，新增/删改站点即红；
//! ③ 数据源注册表 UI 元数据标签（label/credential_label）；
//! ④ main.rs 启动横幅 eprintln（stderr，非 API 面）；
//! ⑤ spa.rs/openapi.rs 的说明性文案；
//! ⑥ console-proxy crate 内文案（本卡文件域外，含 Display 字符串）；
//! ⑦ `src/compile.rs`（44 锚点内含 1 处，但不在本卡文件域
//!    `console/crates/tailrace-console/src/api/*` 内——域外不动，留后续卡；
//!    完整性扫描按路径豁免并在此登记）。
//! ⑧ `src/deploy.rs`（T35 §4.4 部署链，2026-10-24 登记）：deploy_log 的
//!    output 行是落库运行遥测（嵌入远端命令原文，随步骤动态拼装），与
//!    豁免①audit 台账同性质——非 §4.0 信封文案，不进字典；完整性扫描按
//!    路径豁免（exempt 表），站点数无清单钉住（自由拼装的诊断行）。
//! 完整性由 [`tests::no_chinese_format_direct_writes_outside_dictionary`]
//! 钉住：src/ 内（字典模块与 compile.rs 之外）格式化直写 + 中文的行必须
//! 为零。豁免②的 ApiError 字面量残留另由
//! [`tests::api_error_chinese_literals_match_pinned_registry`] 清单制钉住
//! （豁免可查、不可悄悄扩大）。

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
        "err.node.deploy_name_taken" => "节点名称已存在：{0}",
        "err.node.action_unknown" => {
            "未知动作 {0}（支持 start/stop/restart/upgrade/rollback/uninstall）"
        }
        "err.node.remote_cleanup_failed" => {
            "远端清理失败（SSH 链不可达或命令失败）：{0}；已按「跳过并警告」分支仅清理控制面元数据"
        }
        // ---- pipelines ----
        "err.pipeline.not_found" => "管道 #{0} 不存在",
        "err.pipeline.table_objects_missing" => "表对象未登记（先在数据源页登记）: {0}",
        "err.pipeline.statement_table_missing" => {
            "语句 {0} 的{1}表对象未登记: {2}（先在数据源页登记库表）"
        }
        // T-D1 dim joins: ${ds:dim.<dsId>.<table>} references gate at publish
        "err.pipeline.dim_table_missing" => {
            "语句 {0} 的维表引用 {1} 未登记（先在数据源页以 kind=dim 登记维表）"
        }
        "err.pipeline.dim_kind_wrong" => {
            "语句 {0} 的维表 {1} 登记为 kind={2}，维表需以 kind=dim 登记（JDBC 全表广播加载）"
        }
        "err.pipeline.dim_no_sequence" => {
            "语句 {0} 的维表 {1} 未声明键列 sequenceColumn（主键/单列唯一键，全表加载分页键）"
        }
        "err.pipeline.node_no_api_url" => "节点 {0} 未上报 api_url（等待首次心跳）",
        "err.pipeline.engine_assembly_failed" => "引擎装配失败: {0}",
        "err.pipeline.undeployed" => "管道 #{0} 未部署（无指派节点）",
        // 引擎镜像形态（{"ok":false,"error":…} 透传语义，非 §4.0 信封）
        "err.pipeline.undeployed_plain" => "pipeline #{0} 未部署（无指派节点）",
        "err.pipeline.assigned_node_missing" => "指派节点 #{0} 不存在",
        // T-A7 partial-column declarations (INSERT column list) gate at publish
        "err.pipeline.partial_sink_conflict" => {
            "语句 {0} 与语句 {1} 写同一汇 {2} 但部分列声明不一致（同一汇的所有语句须带相同的 INSERT 列清单，或都不带）"
        }
        "err.pipeline.partial_sink_kind" => {
            "语句 {0} 对汇 {1} 声明了部分列更新（INSERT 列清单），但该汇登记为 kind={2}——部分列更新仅支持 doris 汇（UNIQUE KEY merge-on-write 表）"
        }
        "err.pipeline.partial_key_model_missing" => {
            "语句 {0} 对汇 {1} 声明了部分列更新，但该表未登记键模型——Doris 部分列更新仅对 UNIQUE KEY（merge-on-write）表合法，请先登记 keyModel=unique（PUT /api/datasources/tables/<id>，建表向导建的表已自动记录）"
        }
        "err.pipeline.partial_key_model_wrong" => {
            "语句 {0} 对汇 {1} 声明了部分列更新，但该表登记为 keyModel={2}——Doris 部分列更新仅对 UNIQUE KEY（merge-on-write）表合法，DUPLICATE KEY 表的 partial load 会被 Doris 拒绝"
        }
        "err.pipeline.partial_column_unknown" => {
            "语句 {0} 对汇 {1} 的 INSERT 列清单含未登记列 {2}（以该表登记的 columnsJson 为准）"
        }
        // T-A7 mutex, deploy side (live SHOW ALTER TABLE COLUMN)
        "err.pipeline.partial_schema_change" => {
            "表 {0} 有 Schema Change 进行中（{1}）——Doris 不接受该表的部分列更新写入，待 SHOW ALTER TABLE COLUMN 状态为 FINISHED 后再部署"
        }
        "err.pipeline.partial_precheck_failed" => {
            "部分列更新前置检查失败（{0} 的 SHOW ALTER TABLE COLUMN）"
        }
        // LOW-5 (R8): live SHOW CREATE TABLE verdict at publish
        "err.pipeline.partial_live_key_model" => {
            "语句 {0} 对汇 {1} 声明了部分列更新，登记 keyModel=unique 但活表 SHOW CREATE TABLE 核实为 {2}——部分列更新仅对 UNIQUE KEY（merge-on-write）表合法，请核对活表（或以建表向导重建后重新登记）"
        }
        "err.pipeline.partial_live_precheck_failed" => {
            "部分列更新活表核实失败（{0} 的 SHOW CREATE TABLE）"
        }
        // ---- datasources ----
        "err.datasource.not_found" => "数据源 #{0} 不存在",
        "err.datasource.unknown_type" => "未知数据源类型 {0}",
        "err.datasource.unknown_type_registry" => {
            "未知数据源类型 {0}（注册表：kafka/doris/starrocks/file/mysql/postgresql）"
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
        // T-D1 dim 表登记（kind 覆盖 + 键列）
        "err.table_object.kind_override" => {
            "不支持的 kind 覆盖 {0}（登记 kind 由数据源类型派生，仅可选 dim）"
        }
        "err.table_object.dim_kind_datasource" => {
            "维表（kind=dim）仅可登记在 mysql/postgresql 数据源上（JDBC 全表广播加载），当前类型 {0}"
        }
        "err.table_object.dim_sequence_required" => {
            "维表 {0} 必须声明键列 sequenceColumn（主键/单列唯一键，全表加载分页键）"
        }
        // T-A7 key model on record (unique/duplicate)
        "err.table_object.key_model_datasource" => {
            "keyModel 仅可声明在 doris/starrocks 数据源的表上（Doris 键模型概念），当前类型 {0}"
        }
        // ---- T-A1 建表闭环（draft / execute / evolve）----
        "err.ddl.datasource_kind" => {
            "建表目标仅支持 doris/starrocks 数据源（生成的是 Doris 方言 DDL），当前类型 {0}"
        }
        "err.ddl.table_unregistered" => "表对象 {0}.{1} 未在该数据源下登记（先 POST /tables 登记）",
        "err.ddl.columns_missing" => {
            "表对象 {0}.{1} 未登记列清单（先 infer-schema 或登记时带 columnsJson），无法生成/演进 DDL"
        }
        "err.ddl.column_unknown_type" => {
            "列 {0} 的类型 {1} 不在 DDL 生成器支持面内（utf8/string/int8/int16/int32/int64/float32/float64/decimal(p,s)/timestamp(s|ms|us|ns)/date/bool/binary）——不静默折叠"
        }
        "err.ddl.override_column_unknown" => {
            "列类型覆盖 {0} 不在登记列清单中（覆盖仅允许改已登记列的类型，不能新增列）"
        }
        "err.ddl.key_model_required" => {
            "缺少键模型确认：keyModel 必填（unique=UNIQUE KEY 去重 / duplicate=DUPLICATE KEY 追加）——未确认键模型不执行 DDL"
        }
        "err.ddl.key_model_unknown" => "未知键模型 {0}（仅 unique / duplicate）",
        "err.ddl.ddl_missing" => "缺少 ddl 全文（须回传草稿原文执行）",
        "err.ddl.key_model_mismatch" => {
            "键模型 {0} 与回传 DDL 不符（DDL 未含 {1}）——请以同一草稿重新确认后执行"
        }
        "err.ddl.execute_failed" => "DDL 执行失败: {0}",
        "err.ddl.schema_conflict" => {
            "检测到改类型/删列（{0} 处），演进仅支持 ADD COLUMN——建议 DDL 已附于 refs.suggestedDdl，未执行，请人工评估"
        }
        // T-A7 mutex, evolve side
        "err.ddl.partial_update_active" => {
            "表 {0} 上有已部署的部分列更新管道（{1}）——Schema Change 进行中 Doris 会拒绝其 partial load，请先停止这些管道再演进（refs.pipelines）"
        }
        // ---- T-A2 回填（blueprint §7.3）----
        "err.backfill.not_published" => {
            "管道 #{0} 尚未发布（无 spec 快照），先发布再回填"
        }
        "err.backfill.statement_index" => {
            "statementIndex {0} 超出语句范围（该管道共 {1} 条语句，索引从 0 起）"
        }
        "err.backfill.dim_unsupported" => {
            "语句 {0} 含维表 JOIN（{1}）——控制面回填暂不装配维表，请对无维表的语句发起回填"
        }
        "err.backfill.bounds_type" => {
            "回填边界 {0} 的 type={1} 暂不支持（当前仅 offset 边界；timestamp 边界需先解析为 offset）"
        }
        "err.backfill.bounds_value" => {
            "回填边界 {0} 的值 {1} 不是整数（offset 边界须为整数）"
        }
        "err.backfill.bounds_order" => {
            "回填区间非法：from {0} 须小于 to {1}"
        }
        "err.backfill.sink_unsupported" => {
            "回填仅支持 doris/starrocks 汇（重放去重依赖 stream load label 幂等），语句 {0} 的汇 {1} 为 {2}——file/kafka 汇重放会真重复"
        }
        "err.backfill.source_unsupported" => {
            "回填当前仅支持 file 源（kafka 源的 from 需分区级游标寻址，待后续批次支持），语句 {0} 的源 {1} 为 {2}"
        }
        "err.backfill.no_credential" => {
            "汇数据源 #{0} 未存储凭据（回填由控制面直连 stream load，需要已登记凭据）"
        }
        // T-A7 re-gate at backfill accept (the publish-gate diagnostic rides as {0})
        "err.backfill.partial_declaration" => {
            "部分列声明未通过发布门禁，拒绝回填：{0}"
        }
        // ---- T-E2 告警规则（rules CRUD 门禁）----
        "err.alert.name_missing" => "缺少告警规则名称",
        "err.alert.name_taken" => "告警规则名称已存在：{0}",
        "err.alert.type_unknown" => {
            "未知告警规则类型 {0}（lag_seconds / state_failed / heartbeat_age / dlq_window / flush_errors_window）"
        }
        "err.alert.params_invalid" => "告警规则参数非法：{0}",
        "err.alert.webhook_url_invalid" => "webhookUrl 须为 http(s) URL：{0}",
        "err.alert.not_found" => "告警规则 #{0} 不存在",
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
    /// 另钉字典规模 = 82 key（模块文档「规模口径」的机器锚点，T39 ③；T35 +3；
    /// T-D1 dim join +6：发布门禁 3 + 维表登记 3；T-A1 建表闭环 +10；
    /// T-A2 回填 +9：not_published/statement_index/dim_unsupported/
    /// bounds_type/bounds_value/bounds_order/sink_unsupported/
    /// source_unsupported/no_credential；T-A7 部分列更新 +9：发布门禁 5
    /// （sink_conflict/sink_kind/key_model_missing/key_model_wrong/
    /// column_unknown）+ 部署侧互斥 2（schema_change/precheck_failed）+
    /// evolve 侧互斥 1（partial_update_active）+ keyModel 登记 1
    /// （key_model_datasource）；R7 评审修复轮 +1：backfill 部分列重门
    /// （partial_declaration）；LOW-5 +2：publish 活表核实 2
    /// （live_key_model/live_precheck_failed）；T-E2 告警 +6：规则 CRUD 门禁
    /// （name_missing/name_taken/type_unknown/params_invalid/
    /// webhook_url_invalid/not_found））：加 key 时同步改此数与 KEY_LIST。
    #[test]
    fn templates_use_only_positional_placeholders() {
        let keys: Vec<&str> = KEY_LIST.to_vec();
        assert!(!keys.is_empty());
        assert_eq!(keys.len(), 82, "字典规模漂移：同步模块文档「规模口径」");
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
        // 豁免路径：字典模块本身（模板为字面量）、compile.rs（锚点内
        // 含 1 处但文件不在本卡文件域——域外不动项，卡内登记）与
        // deploy.rs（T35 §4.4 部署链）：deploy_log 的 output 行是落库的
        // 运行遥测（嵌入远端 df/tar/engine.log 原文，随步骤动态拼装），
        // 与豁免①的 audit 台账同性质——机器可读操作记录，非 §4.0 信封
        // 用户文案，不进字典（message_key 通道对日志行无意义）。
        let exempt: &[&str] = &["api/messages.rs", "compile.rs", "deploy.rs"];
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

    /// 豁免②机器钉住（R5 评审修复）：ApiError 通道的中文字面量错误响应
    /// 不在 T34 收敛范围（锚点类只覆盖格式化直写），但残留清单必须可被
    /// 机器钉住——api/ 内每个 ApiError 调用切片的中文字面量多重集必须与
    /// 下方登记逐一相等（未登记文件必须为零）。新增/删改任何字面量站点
    /// （含换行写法）都会红，豁免不会被悄悄扩大；收敛某站点进字典时，
    /// 改站点并同步删本表对应条目。
    #[test]
    fn api_error_chinese_literals_match_pinned_registry() {
        let api_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api");
        // 登记口径 = 站点文案多重集（同文案两站点登记两次）；分布见模块
        // 文档豁免②：34 站点 = 19 bad_request/not_found 单字面量
        // + 4 internal+anyhow! + 11 new 三参。
        let pinned: &[(&str, &[&str])] = &[
            (
                "artifacts.rs",
                &[
                    "multipart 缺少 file 字段的文件名",
                    "制品必须是 .tar.gz 包",
                    "创建制品目录失败: {e}",
                    "暂存文件创建失败: {e}",
                    "制品超过 512MB 上限",
                    "写入暂存失败: {e}",
                    "multipart 缺少 file 字段",
                    "缺少 version 字段",
                    "name 仅允许 [A-Za-z0-9._-]",
                    "制品落盘失败: {e}",
                ],
            ),
            ("auth.rs", &["缺少用户名或密码", "用户名或密码错误"]),
            (
                "datasources.rs",
                &[
                    "缺少数据源名称",
                    "connJson 必须是对象",
                    "缺少 generation（CAS 乐观锁必填）",
                    "generation 不匹配，数据源已被其他人修改",
                    "数据源名称已被占用",
                    "connJson 缺少 bootstrap",
                    "connJson 缺少 jdbcUrl",
                    "connJson 缺少 jdbcUrl",
                    "文件数据源不支持发现（路径下的文件请直接登记表对象）",
                    "缺少 dbName / tableName",
                    "缺少 dbName / tableName",
                ],
            ),
            ("nodes.rs", &["节点不存在"]),
            (
                "nodes_admin.rs",
                &[
                    "hosts 不能为空（或改用 nodeId 重新部署既有节点）",
                    "hosts 与 nodeId 二选一",
                    "hosts[].host / hosts[].name 不能为空",
                    "upgrade 需要 artifactId",
                    "缺少 artifactId（制品未指定）",
                    "缺少 hosts 或 nodeId",
                    "缺少 name / host",
                    "节点名称或 node_key 已存在",
                    "节点未上报 api_url（等待首次心跳）",
                    "节点非 SSH 托管（manual/self 注册无部署链身份），仅 deployed 节点可重新部署",
                    "节点非 SSH 托管（manual/self 注册无部署链身份），进程动作不可用",
                ],
            ),
            (
                "pipelines.rs",
                &[
                    "缺少管道名称",
                    "管道名称已存在",
                    "管道名称已存在",
                    "管道尚未发布（无 spec 快照），先执行发布",
                    "没有可指派的在线节点（需心跳 90s 内在线且已上报 api_url）",
                    "节点未上报 api_url",
                    "节点未上报 api_url",
                ],
            ),
        ];
        let mut offenders: Vec<String> = Vec::new();
        for (file, expected) in pinned {
            let text = std::fs::read_to_string(api_dir.join(file)).expect("read api source");
            let mut found: Vec<String> = api_error_call_spans(&text)
                .iter()
                .flat_map(|span| cjk_string_literals(&text[span.clone()]))
                .collect();
            found.sort();
            let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
            want.sort();
            if found != want {
                offenders.push(format!("{file}: found {found:#?}\n{file}: want  {want:#?}"));
            }
        }
        // 未登记文件必须零残留（新文件带 ApiError 中文字面量进域即红）。
        let registered: Vec<&str> = pinned.iter().map(|(f, _)| *f).collect();
        for entry in std::fs::read_dir(&api_dir).expect("read api dir") {
            let path = entry.expect("entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if registered.contains(&name.as_str()) {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read api source");
            let hits: Vec<String> = api_error_call_spans(&text)
                .iter()
                .flat_map(|span| cjk_string_literals(&text[span.clone()]))
                .collect();
            if !hits.is_empty() {
                offenders.push(format!("{name}: 未登记的 ApiError 中文字面量 {hits:#?}"));
            }
        }
        assert!(
            offenders.is_empty(),
            "豁免②清单漂移（收敛站点进字典时同步更新本表）:\n{}",
            offenders.join("\n")
        );
    }

    /// 多行感知负测试入树（R5 评审修复：把开发期瞬态实证固化为可回归
    /// 证据）——`format!(` 与中文字面量分行写的直写必须被
    /// [`format_call_spans`] 取到完整切片且 [`contains_cjk`] 命中；
    /// 英文单行调用保持阴性。
    #[test]
    fn format_call_spans_is_multiline_aware() {
        let sample = concat!(
            "let a = 1;\n",
            "let s = format!(\n",
            "    \"中文 {0}\",\n",
            "    a\n",
            ");\n",
            "let t = format!(\"english only {}\", a);\n",
        );
        let spans = format_call_spans(sample);
        assert_eq!(spans.len(), 2, "两个 format! 调用都要找到");
        let multiline = &sample[spans[0].clone()];
        assert!(multiline.contains('\n'), "切片跨行");
        assert!(multiline.contains("中文 {0}"), "切片覆盖次行中文字面量");
        assert!(contains_cjk(multiline), "多行直写被判中");
        let single = &sample[spans[1].clone()];
        assert!(!contains_cjk(single), "英文单行保持阴性");
    }

    /// 每个格式化宏调用的字符区间（起于 `format!(`，止于配对闭括号）。
    fn format_call_spans(text: &str) -> Vec<std::ops::Range<usize>> {
        let mut spans = Vec::new();
        let mut i = 0;
        while let Some(rel) = text[i..].find("format!(") {
            let start = i + rel;
            match balanced_paren_end(text, start + "format!".len()) {
                Some(end) => {
                    spans.push(start..end);
                    i = end;
                }
                None => break,
            }
        }
        spans
    }

    /// 每个 ApiError 调用的字符区间（`ApiError::ctor(` 起至配对闭括号，
    /// 与 [`format_call_spans`] 同一字符串感知匹配——多行调用同样取全）。
    fn api_error_call_spans(text: &str) -> Vec<std::ops::Range<usize>> {
        let mut spans = Vec::new();
        let mut i = 0;
        while let Some(rel) = text[i..].find("ApiError::") {
            let start = i + rel;
            let mut j = start + "ApiError::".len();
            let bytes = text.as_bytes();
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'(' {
                if let Some(end) = balanced_paren_end(text, j) {
                    spans.push(start..end);
                    i = end;
                    continue;
                }
            }
            i = start + "ApiError::".len();
        }
        spans
    }

    /// 从 `(` 下标起返回配对闭括号后一位（引号内的括号不计数；字节扫描
    /// 对 UTF-8 安全——多字节续字节不与 ASCII 冲突）。
    fn balanced_paren_end(text: &str, open: usize) -> Option<usize> {
        let bytes = text.as_bytes();
        let mut depth = 0usize;
        let mut in_str = false;
        let mut escaped = false;
        let mut j = open;
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
                        return Some(j + 1);
                    }
                }
            }
            j += 1;
        }
        None
    }

    /// 调用切片内的中文字符串字面量（豁免②清单的登记单位，保留重复——
    /// 同文案两站点登记两次）。
    fn cjk_string_literals(call: &str) -> Vec<String> {
        let bytes = call.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] != b'"' {
                i += 1;
                continue;
            }
            let mut j = i + 1;
            while j < bytes.len() {
                if bytes[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if bytes[j] == b'"' {
                    break;
                }
                j += 1;
            }
            let lit = &call[i + 1..j.min(call.len())];
            if contains_cjk(lit) {
                out.push(lit.to_string());
            }
            i = j + 1;
        }
        out
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
        "err.node.deploy_name_taken",
        "err.node.action_unknown",
        "err.node.remote_cleanup_failed",
        "err.pipeline.not_found",
        "err.pipeline.table_objects_missing",
        "err.pipeline.statement_table_missing",
        "err.pipeline.dim_table_missing",
        "err.pipeline.dim_kind_wrong",
        "err.pipeline.dim_no_sequence",
        "err.pipeline.node_no_api_url",
        "err.pipeline.engine_assembly_failed",
        "err.pipeline.undeployed",
        "err.pipeline.undeployed_plain",
        "err.pipeline.assigned_node_missing",
        "err.pipeline.partial_sink_conflict",
        "err.pipeline.partial_sink_kind",
        "err.pipeline.partial_key_model_missing",
        "err.pipeline.partial_key_model_wrong",
        "err.pipeline.partial_column_unknown",
        "err.pipeline.partial_schema_change",
        "err.pipeline.partial_precheck_failed",
        "err.pipeline.partial_live_key_model",
        "err.pipeline.partial_live_precheck_failed",
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
        "err.table_object.kind_override",
        "err.table_object.dim_kind_datasource",
        "err.table_object.dim_sequence_required",
        "err.table_object.key_model_datasource",
        "err.ddl.datasource_kind",
        "err.ddl.table_unregistered",
        "err.ddl.columns_missing",
        "err.ddl.column_unknown_type",
        "err.ddl.key_model_required",
        "err.ddl.key_model_unknown",
        "err.ddl.ddl_missing",
        "err.ddl.key_model_mismatch",
        "err.ddl.execute_failed",
        "err.ddl.schema_conflict",
        "err.ddl.partial_update_active",
        "err.backfill.not_published",
        "err.backfill.statement_index",
        "err.backfill.dim_unsupported",
        "err.backfill.bounds_type",
        "err.backfill.bounds_value",
        "err.backfill.bounds_order",
        "err.backfill.sink_unsupported",
        "err.backfill.source_unsupported",
        "err.backfill.no_credential",
        "err.backfill.partial_declaration",
        // T-E2 alert rules
        "err.alert.name_missing",
        "err.alert.name_taken",
        "err.alert.type_unknown",
        "err.alert.params_invalid",
        "err.alert.webhook_url_invalid",
        "err.alert.not_found",
    ];
}
