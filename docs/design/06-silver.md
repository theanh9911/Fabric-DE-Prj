# 06 — Silver

> Silver = dữ liệu **đúng và dùng được**: kiểu chuẩn, giá trị chuẩn, grain rõ, không trùng, đã hợp nhất nguồn, có lịch sử khi cần.

## 1. Các bước trong mỗi notebook Silver (luôn theo thứ tự này)

```
① chọn input     batch Bronze mới (hoặc toàn bộ khi p_full_reload)
② parse          chuỗi → kiểu (parse_ts, to_amount…) — CSV; ERP đã có kiểu
③ chuẩn hoá      std_* (status, gender, position, brand, tax…)
④ xác thực       khoá bắt buộc, grain, miền giá trị → quarantine / flag
⑤ dedup          theo khoá + tie-breaker
⑥ hợp nhất nguồn wholesale + retail (survivorship)
⑦ ghi            MERGE vào slv.* ; INSERT quarantine
```

Mỗi bước là 1 TEMP VIEW trong `%%sql` → đọc từ trên xuống là hiểu.

## 2. Logic ở đâu

| Loại | Ở đâu | Vì sao |
|---|---|---|
| Hàm dùng chung (parse, làm sạch, `std_*`, `is_sales_recognized`) | SQL function trong `nb_common` | 1 chỗ, có version, review được |
| Logic riêng của 1 bảng (join, survivorship) | `%%sql` trong `nb_slv_<entity>` | Đọc được, gần dữ liệu |
| Việc khó viết gọn bằng SQL (Q10 theo đề) | PySpark trong notebook đó | |
| Mapping cần người vận hành tự sửa không qua release | Bảng (ngoại lệ) | Hiện **không có** nhu cầu này |

Lưu ý: SQL function là **Spark SQL TEMPORARY** — chỉ dùng trong notebook; SQL endpoint/report không gọi được. Report chỉ đọc Gold đã tính sẵn.

## 3. Chuẩn hoá — `std_*`
- `norm_key(s) = upper(regexp_replace(s, '[^A-Za-z0-9]', ''))` gom biến thể hoa/thường, khoảng trắng, dấu câu.
- `std_<miền>(s)` = `CASE norm_key(s) WHEN … THEN 'Chuẩn' … ELSE NULL END`. Danh sách nhánh lấy từ EDA (Bước 2); `levenshtein()` chỉ **gợi ý** biến thể khi EDA.
- Giá trị không nhận ra → NULL → **quarantine** với lý do `unknown_<miền>: <giá trị gốc>` → thêm nhánh vào function → chạy lại từ Bronze.

## 4. Mỗi bảng phải ghi rõ

| Bảng | Grain / khoá | Trùng & tie-breaker | Đến trễ / update | Xoá |
|---|---|---|---|---|
| `slv_orders_history` | `order_no, product_code, order_status, updated_at` | trùng y hệt → 1 dòng | insert-only | — |
| `slv_orders_current` (PySpark, Q10) | `order_no, product_code` | `updated_at` mới nhất; hoà → `status_sequence` cao hơn; hoà → wholesale | chỉ update khi `updated_at` mới hơn bản đang có | — |
| `slv_customer` | `customer_code` | survivorship **theo cột**: giá trị khác NULL có `updated_at` mới nhất; hoà → wholesale (ADR 003) | MERGE theo `_record_hash` | vắng trong snapshot mọi nguồn → `is_deleted` |
| `slv_product` | `product_code` | wholesale ưu tiên | như trên | như trên |
| `slv_category` | `category_code` | `CAT005`: bản snapshot mới nhất; tách đường dẫn → `lvl1..lvl4` | | như trên |
| `slv_sales_hierarchy` | `source_system, salesman_code` | snapshot mới nhất | | như trên |

## 5. Dòng có vấn đề

| Mức | Khi nào | Đi đâu |
|---|---|---|
| quarantine | Không parse được, thiếu khoá bắt buộc, giá trị chuẩn hoá ra NULL | `slv.quarantine_<entity>`: cột gốc + `dq_reason` + `_batch_id` + `_run_id` |
| flag | Dùng được nhưng đáng ngờ: outlier, orphan FK, ngày tương lai | Ở lại Silver, `dq_flags ARRAY<STRING>` |
| fail batch | Lỗi toàn cục (xem 08) | Dừng |

Đưa dòng quarantine trở lại: sửa function → chạy lại Silver cho batch đó từ Bronze.

## 6. Lịch sử
- `updated_at` của ERP master **không** phải lịch sử (ERP chỉ giữ hiện tại). Lịch sử master lấy từ **chuỗi snapshot Bronze** (mỗi ngày 1 ảnh).
- Orders có lịch sử sự kiện thật (mỗi trạng thái 1 dòng) → `slv_orders_history`.

## Quyết định
- Luật trong code (`nb_common`), không bảng ref (ADR 013). Tie-breaker và survivorship như §4.

## Còn mở
- Ngưỡng outlier (p99? IQR?) — chốt sau EDA.
