# Hiểu Source (CompanyA-Source)

> Source **giả lập hệ thống của team khác** để Platform có dữ liệu "sống": mỗi ngày phát sinh đơn mới, đơn cũ đổi trạng thái, master thỉnh thoảng đổi. Platform **chỉ đọc**, không bao giờ ghi vào Source.

## 1. Ba khối trong workspace

```
CompanyA-Source
│
├── lh_sim                    ① KHO GỐC (hậu trường — Platform KHÔNG đọc)
│   ├── Files/seed/*.csv          9 file CSV gốc của đề bài (upload tay 1 lần)
│   ├── seed_<source>_<entity>    9 bảng: nguyên dữ liệu gốc + cột _release_date (ngày dòng "xuất hiện")
│   ├── sim_state                 virtual clock: released_until = ngày giả lập hiện tại
│   ├── sim_release_log           mỗi lần chạy thả bao nhiêu dòng / nguồn
│   └── sim_reject_log            dòng bị "DB từ chối" (vi phạm NOT NULL / PK)
│
├── sqldb_erp_wholesale       ② HỆ THỐNG ERP (nguồn wholesale) — Platform đọc
│   ├── dbo.customers / products / sales_hierarchy / orders
│   └── sim.stg_*                 bảng tạm nội bộ của simulator (Platform không đọc)
│
└── lh_retail_drop            ③ DROP ZONE FILE (nguồn retail + reference) — Platform đọc
    └── Files/inbound/
        ├── retail/{customers,products,sales_hierarchy,orders}/*.csv
        └── reference/categories/*.csv
```

| Khối | Ngoài đời tương ứng |
|---|---|
| ① `lh_sim` | "Tương lai" của doanh nghiệp — đã biết trước, thả dần ra |
| ② ERP | Hệ thống bán buôn: CSDL quan hệ, có PK, có kiểu dữ liệu, cập nhật tại chỗ |
| ③ Drop zone | Hệ thống bán lẻ không cho kết nối DB → mỗi ngày **gửi file CSV** vào 1 thư mục chung; `categories` là file tham chiếu dùng chung |

## 2. Ba notebook simulator

| Notebook | Chạy khi | Làm gì |
|---|---|---|
| `nb_00_sim_setup` | 1 lần | Đọc 9 CSV gốc → bảng `seed_*`, gắn `_release_date` cho từng dòng: orders = ngày của `updated_at`; master = ngày mới nhất của `created/inserted/updated_at`; ngày không đọc được hoặc sau 2026-12-31 (ngày sai) → `1900-01-01` (thả ngay ở lần đầu) |
| `nb_00_sim_daily` | mỗi ngày | Thả các dòng có `_release_date` trong cửa sổ `(ngày hiện tại, ngày hiện tại + p_days]` → ERP & drop zone; tiến virtual clock; trả về ngày mới (exit value) |
| `nb_00_sim_reset` | khi muốn làm lại từ đầu | Xoá dữ liệu ERP, file inbound, state, log. Giữ seed. ⚠️ Reset Source thì Platform cũng phải reset state |

## 3. Virtual clock — Source đang ở ngày nào?

```
1900-01-01 ─────────────── 2025-12-31 │ 2026-01-01 │ 2026-01-02 … ~2026-05-10 (hết seed)
     lần chạy đầu (initial load)     │ ngày 1     │ ngày 2 …
     thả toàn bộ lịch sử             │ +1 ngày mỗi lần chạy nb_00_sim_daily
                                     ▲
                          HIỆN TẠI: released_until = 2026-01-01
```

- Dữ liệu giữ **ngày gốc** (đơn ngày 2026-01-01 có `updated_at = 2026-01-01 …`). Chỉ có *thời điểm thả ra* là do clock quyết định.
- Ngày thật và ngày giả lập **không liên quan**: hôm nay (thật) 2026-10-06, Source (giả lập) 2026-01-01.
- Chạy lại cùng ngày → không nhân đôi (idempotent). Không lùi clock được — muốn làm lại thì `nb_00_sim_reset`.

## 4. Mỗi lần thả, dữ liệu vào nguồn ra sao

### ERP (`wholesale`)
| Entity | Cách ghi | Ghi chú |
|---|---|---|
| customers, products, sales_hierarchy | **MERGE** theo khoá (thêm mới / cập nhật tại chỗ) | ERP chỉ giữ **trạng thái hiện tại** — không có lịch sử |
| orders | xoá-rồi-chèn các dòng có `updated_at` trong cửa sổ | 1 dòng = 1 dòng sản phẩm của đơn (`order_line_id` tự tăng) |

**ERP "hybrid" (ADR 008):** cột thời gian ép về `datetime2`, `quantity`/`price` ép về số — **giá trị không ép được thành NULL**; cột chữ giữ nguyên giá trị bẩn (status sai chính tả, tax `five percent`…). Có PK; khoá ngoại khai báo nhưng **không kiểm** (orphan vẫn vào được). Dòng vi phạm NOT NULL / trùng PK bị "DB từ chối" → `sim_reject_log` (hiện = 0).

### File (`retail`, `reference`)
| Entity | File | Ghi chú |
|---|---|---|
| orders | lần đầu: `orders_history_until_20251231.csv`; sau đó **1 file / ngày**: `orders_YYYYMMDD.csv` | chỉ chứa dòng mới/đổi của ngày đó |
| customers, products, sales_hierarchy, categories | `<entity>_YYYYMMDD.csv` — chỉ khi có thay đổi | **snapshot đầy đủ** (toàn bộ bảng tới ngày đó), không phải phần thay đổi |

File giữ **nguyên văn** dữ liệu gốc: mọi giá trị là chữ, ngày nhiều định dạng, khoảng trắng thừa giữ nguyên.

## 5. Số liệu hiện có (từ `sim_release_log`)

| Lần thả | wholesale (ERP) | retail (file) | reference |
|---|---|---|---|
| Initial → 2025-12-31 | orders 41.874 · customers ~20 · products 22 · sales_hierarchy 20 | orders 42.194 · customers 20 · products 64 · sales_hierarchy 20 | categories 10 |
| 2026-01-01 | orders +66 · master 0 | orders +64 · master 0 | 0 |

Master rất nhỏ (vài chục dòng); orders ~62 dòng / nguồn / ngày.

## 6. Dữ liệu cố ý bẩn — Platform phải xử lý

| Loại | Ví dụ | Xử lý ở |
|---|---|---|
| Ngày nhiều định dạng (file) | `3/15/2025 9:30`, `25/3/2025`, `2025-03-15` | Silver: `parse_ts` |
| Ngày sai / tương lai | ~40 dòng năm **2027** | DQ future_date |
| Status sai chính tả | biến thể của Delivered/Cancelled… | `ref_value_mapping` |
| Số dạng chữ | tax `five percent`; qty âm / thập phân | Silver quarantine |
| `"NULL"` dạng chữ | `"NULL"` trong ô | `clean_text` → NULL |
| Orphan | `CUS099`, `PRD999`, `CAT010`, `CAT999` | Gold: unknown member `-1` |
| Trùng | `CAT005` 2 bản; cùng khách ở 2 nguồn khác thuộc tính; master gửi lại dạng snapshot | Silver dedup / survivorship |

Danh sách chính xác + số lượng sẽ có ở Bước 2 (`dq_findings.md`), đo trên Bronze.

## 7. Hệ quả cho Platform

| Đặc điểm Source | Platform phải… |
|---|---|
| ERP chỉ giữ trạng thái hiện tại | tự giữ lịch sử (Bronze append, SCD2 ở Gold) |
| `updated_at` ERP là **giờ giả lập** | watermark ERP = `max(updated_at)` dữ liệu, không phải giờ chạy ([1.5](01-ingest/05-watermark.md)) |
| File được ghi vào drop zone theo **giờ thật** | watermark file = LastModified / `run_start` |
| Master dạng file là snapshot đầy đủ | Silver lấy bản mới nhất theo khoá, không cộng dồn |
| ERP ép kiểu thất bại → NULL | dòng ERP có thể mất thông tin (vd qty) — so với file retail để phát hiện |
| Có dòng ngày 2027 | không để chúng đẩy watermark ([1.5](01-ingest/05-watermark.md)) |

## 8. Xem trạng thái Source

Trong ws Source → `lh_sim` → SQL analytics endpoint → New SQL query:
```sql
SELECT * FROM sim_state;                                         -- ngày giả lập hiện tại
SELECT * FROM sim_release_log ORDER BY logged_at DESC;           -- lịch sử thả
SELECT * FROM sim_reject_log;                                    -- dòng bị ERP từ chối
```
Trong `sqldb_erp_wholesale` → New query:
```sql
SELECT count(*) AS n, min(updated_at) AS min_upd, max(updated_at) AS max_upd FROM dbo.orders;
```
