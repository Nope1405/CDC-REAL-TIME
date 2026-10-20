# CDC Search Sync — PostgreSQL → Debezium → Kafka → Python → Elasticsearch

Tài liệu tham chiếu cho dự án: các quyết định thiết kế, cấu hình bắt buộc, những lỗi hay gặp và checklist hoàn thành.
Các mục đánh dấu **[BỔ SUNG]** là phần còn thiếu so với kế hoạch ban đầu.

---

## 0. Mục tiêu và tiêu chí hoàn thành [BỔ SUNG]

Kế hoạch cũ chưa có con số "thế nào là đạt". Cần chốt trước khi benchmark:

| Chỉ số | Mục tiêu đề xuất (dev, 1 máy) |
|---|---|
| End-to-end lag p95 (PG commit → ES searchable) | < 2s khi tải đều 500 update/s |
| End-to-end lag p99 | < 5s |
| Throughput consumer | ≥ 5.000 doc/s (bulk) |
| Mất dữ liệu sau khi kill consumer / ES / Connect | 0 bản ghi (count + checksum khớp) |
| Ảnh hưởng lên TPS của pgbench khi bật CDC | giảm < 5–10% |
| Rebuild index 100k products từ topic | ghi lại thời gian, không downtime nhờ alias |

---

## 1. Quyết định thiết kế

| Quyết định | Lựa chọn | Ghi chú |
|---|---|---|
| Raw CDC hay Outbox | Raw CDC cho `products`; Outbox cho `orders` (+ `order_items`) | Xem mục 3.2 |
| Định dạng message | JSON trước, Avro + Schema Registry là phần nâng cao | Với JSON, tắt schema: `value.converter.schemas.enable=false` để message gọn |
| Ngữ nghĩa giao nhận | At-least-once + idempotent upsert | PK làm `_id`, commit offset sau khi bulk thành công |
| Thứ tự | Kafka key = PK → cùng partition | ES external versioning bằng LSN |
| Retention topic | **[BỔ SUNG]** `cleanup.policy=compact` cho topic CDC | Compaction giữ bản mới nhất mỗi key vô thời hạn → replay rebuild được mà không cần retention "đủ dài" |

### 1.1 Chi tiết về versioning [BỔ SUNG — lỗi hay gặp]

- Dùng **`source.lsn`** thay vì `updated_at`: `updated_at` có thể trùng (cùng timestamp), lệch (do app set), hoặc không được cập nhật nếu ai đó quên trigger.
- Nếu dùng SMT `unwrap`, LSN biến mất khỏi payload → phải thêm `add.fields=op,source.ts_ms,source.lsn,ts_ms`.
- Dùng `version_type=external_gte` thay vì `external` để replay cùng message không bị lỗi. Nếu vẫn dùng `external`, phải coi **HTTP 409 version_conflict là thành công** (bản mới hơn đã có), không đưa vào retry/DLQ.
- Delete cũng phải gửi kèm version. Lưu ý ES chỉ nhớ version của doc đã xóa trong `index.gc_deletes` (mặc định 60s): nếu một event cũ (create) đến muộn hơn 60s sau delete, doc sẽ "sống lại". Với at-least-once và replay, cân nhắc **soft delete** (`is_deleted=true` + filter khi search) thay vì xóa cứng.
- Snapshot (`op=r`) mang LSN tại thời điểm snapshot, luôn nhỏ hơn các event streaming sau đó → an toàn.

---

## 2. Hạ tầng (Docker Compose)

Service: `postgres`, `kafka` (KRaft), `kafka-connect` (Debezium), `elasticsearch`, `kibana`, `kafka-ui`/Redpanda Console, tùy chọn `prometheus` + `grafana`.

### 2.1 Những điểm còn thiếu [BỔ SUNG]

- **Pin version image**, không dùng `latest`. Từ Debezium 3.x image chính thức nằm ở `quay.io/debezium/...` (Docker Hub không còn cập nhật).
- **Kafka 1 broker**: phải đặt replication factor = 1 cho mọi topic nội bộ, nếu không sẽ treo:
  - `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`
  - Kafka Connect: `CONFIG_STORAGE_REPLICATION_FACTOR=1`, `OFFSET_STORAGE_REPLICATION_FACTOR=1`, `STATUS_STORAGE_REPLICATION_FACTOR=1`
- **Advertised listeners**: tách listener nội bộ (`kafka:29092` cho container) và listener ngoài (`localhost:9092` cho script Python chạy trên host). Đây là lỗi kết nối phổ biến nhất.
- **Healthcheck + `depends_on: condition: service_healthy`** để Connect không khởi động trước Kafka/PG, và `register.sh` chờ Connect sẵn sàng (`curl :8083/connectors` trả 200).
- **ES single-node**: `discovery.type=single-node`, `xpack.security.enabled=false`, `ES_JAVA_OPTS=-Xms1g -Xmx1g`, và `number_of_replicas=0` trong mapping (nếu không cluster sẽ luôn yellow).
- **Volume** cho PG, Kafka, ES để dữ liệu sống qua restart; script `make reset` để xóa sạch khi cần.
- **File `.env`** chứa mật khẩu/port; không hardcode trong compose hay connector JSON.

### 2.2 Lưu ý riêng cho Windows [BỔ SUNG]

- Docker Desktop dùng WSL2: giới hạn RAM nằm ở `%UserProfile%\.wslconfig` (`memory=10GB`). Không chỉnh thì ES/Connect có thể bị OOM.
- ES cần `vm.max_map_count=262144`: chạy `wsl -d docker-desktop sysctl -w vm.max_map_count=262144`.
- Script `.sh` mount vào container phải dùng **LF**, không phải CRLF → thêm `.gitattributes`: `*.sh text eol=lf`.
- Đường dẫn có dấu cách và `&` (`CDC &Real-Time`) dễ làm hỏng script shell/Makefile; nên đổi tên thư mục hoặc luôn quote path.

---

## 3. PostgreSQL

### 3.1 Cấu hình

```
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
max_slot_wal_keep_size = 2GB      # PG13+, chống slot giữ WAL làm đầy đĩa
```

- User riêng có `REPLICATION` + `SELECT` trên bảng theo dõi.
- `CREATE PUBLICATION ... FOR TABLE ...`; trên Debezium đặt `publication.autocreate.mode=disabled` (hoặc `filtered`) để không cần quyền superuser.
- `REPLICA IDENTITY FULL` cho bảng cần before-image.
- Schema mẫu `products`, `orders`, `order_items` có `updated_at` + trigger tự cập nhật.

### 3.2 Orders: Outbox hay raw CDC [BỔ SUNG chi tiết]

Kế hoạch chỉ nói "cân nhắc Outbox" nhưng chưa có thiết kế cụ thể:

- Bảng `outbox_events(id uuid, aggregate_type, aggregate_id, type, payload jsonb, created_at)`.
- Service ghi `orders` + `order_items` + 1 dòng `outbox_events` (payload là order đầy đủ kèm items) **trong cùng transaction**.
- Debezium SMT `io.debezium.transforms.outbox.EventRouter`: `route.by.field=aggregate_type`, key = `aggregate_id`.
- Dọn bảng outbox: xóa ngay sau khi insert (Debezium vẫn đọc được từ WAL) hoặc job xóa định kỳ, nếu không bảng phình to.
- Nếu vẫn chọn raw CDC cho orders: thay đổi ở `order_items` phải cập nhật lại doc order → hoặc consumer truy vấn PG để dựng lại order (đơn giản, nhưng tải lên DB chính), hoặc dùng ES nested + scripted update. Có thể bật `provide.transaction.metadata=true` để gom các event cùng transaction.
- **Bài toán fan-out**: nếu doc order chứa `product_name`, khi đổi tên product thì có cập nhật hàng nghìn order cũ không? Cần ghi rõ quyết định (thường là **không**, giữ snapshot tên tại thời điểm đặt hàng).

### 3.3 Giám sát slot [BỔ SUNG]

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag,
       wal_status, safe_wal_size
FROM pg_replication_slots;
```

`wal_status = lost` nghĩa là slot đã vượt `max_slot_wal_keep_size` và bị vô hiệu → connector phải snapshot lại. Cần có runbook cho trường hợp này (mục 9).

---

## 4. Debezium Connector

Các mục đã có: `plugin.name=pgoutput`, `slot.name`, `publication.name`, `topic.prefix`, `table.include.list`, `snapshot.mode=initial`, SMT `unwrap`, `decimal.handling.mode=string`, `time.precision.mode`, heartbeat.

### 4.1 Còn thiếu [BỔ SUNG]

- **TOAST**: cột text/jsonb lớn không đổi trong UPDATE sẽ có giá trị `__debezium_unavailable_value` (khi không dùng `REPLICA IDENTITY FULL`). Consumer phải bỏ qua field có giá trị này (partial update) thay vì ghi đè vào ES. Đây là bug âm thầm rất hay gặp với cột `description`.
- **Cấu hình unwrap đầy đủ**:
  ```json
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,source.ts_ms,source.lsn,ts_ms"
  ```
  (Debezium 3.x thay `delete.handling.mode` bằng `delete.tombstone.handling.mode`; kiểm tra theo version đang dùng.)
- **Tombstone + compaction**: `tombstones.on.delete=true` để compaction dọn được key đã xóa.
- **`time.precision.mode=connect`** hoặc `adaptive_time_microseconds`; `timestamptz` ra chuỗi ISO‑8601 → mapping ES dùng `date` với `strict_date_optional_time`. Ghi rõ timezone (UTC).
- **`heartbeat.action.query`** (ví dụ update một bảng `debezium_heartbeat`) khi DB có nhiều bảng không theo dõi thay đổi liên tục: chỉ `heartbeat.interval.ms` là không đủ để slot tiến lên trong trường hợp này.
- **Incremental snapshot qua signal table** (`signal.data.collection`): cho phép snapshot lại 1 bảng mà không dừng streaming. Hữu ích khi rebuild hoặc thêm bảng mới vào `table.include.list`.
- **Xử lý lỗi trong Connect**: `errors.tolerance`, `errors.log.enable`. Lưu ý: `errors.deadletterqueue.*` của Kafka Connect **chỉ áp dụng cho sink connector**; Debezium là source connector nên DLQ phải nằm ở consumer (đã làm trong `consumer/app/dlq.py`).
- **`snapshot.fetch.size`**, `max.batch.size`, `max.queue.size` để tune throughput.
- `slot.drop.on.stop=false` (mặc định) — không đặt `true` ở môi trường mô phỏng production.

---

## 5. Kafka

- Topic `<prefix>.<schema>.<table>`, 3–6 partition.
- **[BỔ SUNG]** Tạo topic chủ động (script) thay vì để auto-create, để kiểm soát partition và `cleanup.policy`. Hoặc dùng `topic.creation.default.*` trong connector.
- **[BỔ SUNG]** Thay đổi số partition sau khi đã chạy sẽ làm hỏng đảm bảo thứ tự theo key → chốt từ đầu.
- DLQ topic: `cdc.dlq.products`. **[BỔ SUNG]** Message DLQ cần header: lỗi, stacktrace rút gọn, topic/partition/offset gốc, thời điểm. Cần script `replay_dlq.py` để đẩy lại sau khi sửa lỗi.
- **[BỔ SUNG]** `max.message.bytes` nếu payload order (kèm items) lớn.

---

## 6. Elasticsearch

- Mapping tường minh, alias `products` → `products_v1`, `refresh_interval` 1s là sàn của độ trễ hiển thị.

### 6.1 Còn thiếu [BỔ SUNG]

- **`dynamic` của mapping**: `strict` (field lạ → lỗi → DLQ) hay `false` (bỏ qua, vẫn lưu trong `_source`). Quyết định này ảnh hưởng trực tiếp tới test schema evolution. Đề xuất `false` + cập nhật mapping chủ động.
- **Analyzer tiếng Việt**:
  ```json
  "filter": { "vn_folding": { "type": "asciifolding", "preserve_original": true } },
  "analyzer": { "vn": { "tokenizer": "standard", "filter": ["lowercase", "vn_folding"] } }
  ```
  `preserve_original` giúp truy vấn có dấu vẫn ưu tiên khớp chính xác. Cân nhắc thêm `search_as_you_type` hoặc edge n‑gram cho autocomplete; synonym ("đt" ↔ "điện thoại") là phần mở rộng.
- **Index template** cho `products_v*` để mọi phiên bản index có cùng setting.
- **Quy trình reindex không downtime** (chưa có trong kế hoạch):
  1. Tạo `products_v2` với mapping mới, `refresh_interval=-1`, `number_of_replicas=0`.
  2. Chạy consumer với **consumer group mới**, đọc từ đầu topic, ghi vào `products_v2`.
  3. Khi lag ≈ 0: bật lại `refresh_interval`, đổi alias **nguyên tử** (`_aliases` với `remove` + `add` trong 1 request).
  4. Chuyển consumer chính sang v2, dừng consumer cũ, xóa v1 sau.
- Field đo lường `_cdc_source_ts`, `_indexed_at`: lưu ý tên bắt đầu bằng `_` dễ nhầm với metadata field của ES; nên đặt `cdc_source_ts`, `indexed_at` hoặc gom vào object `cdc.*`.

---

## 7. Python Consumer

Đã có: batch theo số lượng/thời gian, `enable.auto.commit=false`, map op, retry backoff, DLQ, graceful shutdown, field đo lường.

### 7.1 Còn thiếu [BỔ SUNG]

- **Rebalance**: đăng ký `on_revoke` để flush batch + commit trước khi mất partition; nếu không sẽ xử lý trùng hoặc commit offset của partition không còn sở hữu.
- **Commit đúng offset**: commit `offset + 1` theo từng partition, dùng `consumer.commit(offsets=..., asynchronous=False)` sau bulk.
- **Phân loại lỗi từng item trong bulk** (`helpers.bulk(..., raise_on_error=False)` hoặc `streaming_bulk`):
  - 409 version conflict → coi là thành công
  - 400 (mapping/parse) → DLQ, không retry
  - 429 / 5xx / timeout → retry có backoff + jitter
  - Chỉ commit khi mọi item đã thành công hoặc đã vào DLQ.
- **Retry vô hạn vs `max.poll.interval.ms`**: khi ES chết lâu, vòng retry làm consumer quá hạn poll và bị kick khỏi group. Giải pháp: `consumer.pause()` các partition, vẫn gọi `poll()` định kỳ, `resume()` khi ES sống lại.
- **Gộp trong batch**: nhiều event cùng key trong 1 batch → chỉ giữ event có LSN lớn nhất (giảm tải ES và tránh ghi đè ngược trong cùng bulk).
- **Tombstone**: value = `null` → delete theo key; message delete có `__deleted=true` (khi dùng `rewrite`).
- **Pydantic**: `model_config = ConfigDict(extra="allow")` để cột mới không làm hỏng validate; `price` nhận từ chuỗi → `Decimal`.
- **Producer DLQ**: `enable.idempotence=true`, gọi `flush()` trước khi commit offset của message gốc.
- **Config qua biến môi trường** (pydantic-settings): bootstrap servers, topic, batch size, flush interval, ES URL.
- **Health endpoint** (`/healthz`, `/metrics`) để Docker healthcheck và Prometheus scrape.
- **Metrics** cần xuất: `events_consumed_total{op}`, `bulk_duration_seconds` (histogram), `bulk_errors_total{type}`, `dlq_total`, `e2e_lag_seconds` (histogram, = `indexed_at - source.ts_ms`), `batch_size`.

---

## 8. Test và Benchmark

Đã có: định nghĩa lag theo chặng, 3 kịch bản tải, p50/p95/p99, consumer lag, `MilliSecondsBehindSource`, pgbench có/không CDC, 5 bài test chịu lỗi.

### 8.1 Còn thiếu [BỔ SUNG]

- **Test tự động**:
  - Unit test: mapping event → bulk action (c/u/r/d/tombstone/TOAST placeholder/decimal) với **file sample event Debezium thật** lưu trong `consumer/tests/fixtures/`.
  - Integration test: `testcontainers` (Kafka + ES) hoặc chạy trên compose, kiểm tra end-to-end 1 insert/update/delete.
- **Script kiểm tra nhất quán** `verify_consistency.py`: so count và checksum (ví dụ hash của `id, price, updated_at`) giữa PG và ES, liệt kê id lệch. Đây là công cụ chính cho mọi bài test chịu lỗi, nhưng chưa có trong cấu trúc repo.
- **Đo "searchable" chính xác**: sau khi update, poll `_search` theo id + version cho tới khi thấy giá trị mới; hoặc tính `indexed_at + refresh_interval` là cận trên. Ghi rõ dùng cách nào.
- **Đồng hồ**: mọi thành phần chạy cùng máy nên clock chung; nếu tách máy phải có NTP, nếu không lag âm/sai.
- **Warm-up** trước khi đo, bỏ vài giây đầu; chạy mỗi kịch bản ≥ 3 lần, báo cáo trung vị.
- **pgbench**: dùng custom script (`-f update_products.sql`) update đúng bảng `products`, không chỉ bảng mặc định của pgbench. So sánh 3 trường hợp: không CDC / CDC chạy bình thường / slot tồn tại nhưng connector dừng (WAL tích tụ).
- **Bài test chịu lỗi bổ sung**:
  - Kill Kafka broker rồi bật lại.
  - Gửi message hỏng thủ công vào topic → phải vào DLQ, consumer không dừng.
  - Slot vượt `max_slot_wal_keep_size` (`wal_status=lost`) → quy trình khôi phục.
  - Restart Kafka Connect trong lúc đang snapshot.
  - Drop cột / đổi kiểu cột (không chỉ thêm cột).
- **Ghi lại môi trường** trong `results.md`: CPU, RAM cấp cho Docker, version các image, cấu hình batch.

---

## 9. Vận hành / Runbook [BỔ SUNG — chưa có trong kế hoạch]

`docs/runbook.md` nên có các quy trình:

- Kiểm tra trạng thái connector: `GET /connectors/<name>/status`; restart task lỗi: `POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true`.
- Reset offset consumer group: `kafka-consumer-groups --reset-offsets --to-earliest --execute`.
- Slot bị `lost` hoặc phải drop khẩn cấp vì đĩa đầy: `SELECT pg_drop_replication_slot(...)`, sau đó snapshot lại.
- Replay DLQ.
- Rebuild index (mục 6.1).
- Cảnh báo Prometheus: slot lag > 1GB, consumer lag > N, `MilliSecondsBehindSource` > 30s, DLQ tăng, connector task FAILED.

### 9.1 Observability [BỔ SUNG chi tiết]

- Debezium/Connect metric qua JMX → cần **JMX exporter (javaagent)** gắn vào container Connect; không tự có.
- `kafka-exporter` (hoặc metric của Redpanda Console) cho consumer lag.
- `postgres_exporter` với custom query về `pg_replication_slots`.
- `elasticsearch-exporter` (tùy chọn).
- Lưu dashboard Grafana dưới dạng JSON trong repo (provisioning).

---

## 10. Cấu trúc repo (cập nhật)

```
cdc-search-sync/
├── docker-compose.yml
├── .env.example                  [BỔ SUNG]
├── .gitattributes                [BỔ SUNG] *.sh eol=lf
├── Makefile                      [BỔ SUNG] up, down, reset, register, seed, bench, verify
├── postgres/        init.sql, postgresql.conf, outbox.sql [BỔ SUNG]
├── kafka/           create-topics.sh [BỔ SUNG]
├── connect/         debezium-products.json, debezium-orders-outbox.json [BỔ SUNG], register.sh
│                    jmx-exporter/ [BỔ SUNG]
├── elasticsearch/   products-mapping.json, orders-mapping.json,
│                    index-template.json [BỔ SUNG], reindex.sh [BỔ SUNG]
├── consumer/        app/, Dockerfile, requirements.txt, tests/ (fixtures/ với sample event)
├── monitoring/      prometheus.yml, alerts.yml, grafana/dashboards/ [BỔ SUNG]
├── benchmark/       seed.py, load_update.py, measure_lag.py,
│                    verify_consistency.py [BỔ SUNG], chaos/ [BỔ SUNG], report.ipynb
└── docs/            architecture.md, results.md, runbook.md [BỔ SUNG], adr/ [BỔ SUNG]
```

---

## 11. Kiến thức cần nắm

| Mảng | Nội dung cần hiểu |
|---|---|
| PostgreSQL | WAL, logical decoding, `pgoutput`, replication slot, publication, replica identity, TOAST |
| Debezium | Envelope (before/after/source/op), snapshot vs streaming, SMT, outbox event router, incremental snapshot, offset lưu trong Connect |
| Kafka | Partition và thứ tự theo key, consumer group, rebalance, offset commit, log compaction, KRaft |
| Elasticsearch | Mapping, analyzer, bulk API, external versioning, refresh, alias, reindex |
| Python | confluent-kafka (poll/commit/pause/resume, callback rebalance), helpers.bulk, pydantic v2, xử lý signal |
| Hệ thống phân tán (DDIA ch.11–12) | Derived data, at-least-once + idempotence, log làm nguồn sự thật, replay, độ trễ vs nhất quán |
| Đo lường | Percentile, histogram, Little's law, phân biệt latency và throughput |

---

## 12. Checklist triển khai theo thứ tự

1. [ ] Compose chạy được, mọi service healthy, có `.env`, pin version
2. [ ] PG: schema, publication, user replication, `max_slot_wal_keep_size`, seed 100k products
3. [ ] Connector products (raw CDC, unwrap + add.fields LSN, decimal string) → thấy message trong Kafka UI
4. [ ] ES: template, mapping, analyzer tiếng Việt, alias
5. [ ] Consumer tối thiểu: batch → bulk → commit; idempotent theo `_id`
6. [ ] Versioning bằng LSN, xử lý 409, delete/tombstone, TOAST placeholder
7. [ ] DLQ + phân loại lỗi + pause/resume khi ES chết + rebalance callback + graceful shutdown
8. [ ] Metrics + Prometheus + Grafana + cảnh báo slot lag
9. [ ] `verify_consistency.py`
10. [ ] Orders qua Outbox
11. [ ] Benchmark 3 kịch bản + pgbench
12. [ ] Test chịu lỗi + schema evolution + rebuild index bằng replay
13. [ ] Viết `results.md`, `runbook.md`, ADR
14. [ ] (Nâng cao) Avro + Schema Registry
