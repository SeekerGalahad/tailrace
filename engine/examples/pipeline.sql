-- tailrace pipeline example: statement set (a pipeline = >=1 INSERT INTO sink
-- SELECT ... FROM source).
-- Scenario: CDC orders feed — one OGG topic written into a Doris ODS table and
-- a readiness notification sent to a Kafka topic.
-- Note: notification is not an engine mechanism — it is just a second plain
-- INSERT (its table object is the notification topic).
-- ${ds:id.db.table} references table objects; credentials never enter SQL/IR
-- (the runtime fetches them from the environment just-in-time).

-- Statement 1: data lands in the Doris ODS (cursor group tr.p42.s0)
INSERT INTO ${ds:3.shop.orders}
SELECT
    after.order_no                AS order_no,
    after.batch_no                AS batch_no,
    after.line_id                 AS line_id,
    after.station                 AS station,
    after.result                  AS result,
    op_type                       AS op_type,
    ptime                         AS etl_time
FROM ${ds:12.default.orders_ogg}
WHERE after.table = 'SHOPDB.ORDERS'
  AND op_type IN ('I', 'U');

-- Statement 2: readiness notification (cursor group tr.p42.s1, key=batch_no
-- preserves ordering; the envelope schema stays compatible with existing
-- consumers)
INSERT INTO ${ds:15.default.orders_notify}
SELECT
    after.order_no                AS batch_key,
    'ODS_READY'                   AS event_type,
    'shop.orders'                 AS target_table,
    now()                         AS event_time
FROM ${ds:12.default.orders_ogg}
WHERE after.table = 'SHOPDB.ORDERS'
  AND op_type IN ('I', 'U');
