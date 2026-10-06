# 0.4 — `nb_common`: hàm làm sạch dùng chung ✅

## Mục tiêu
Các hàm SQL dùng được trong mọi cell `%%sql` của notebook Silver/Gold.

## Hiểu trước khi làm
- Định nghĩa **1 lần** ở đây, notebook khác gọi `%run nb_common` → không lặp code (nguyên tắc P4).
- Hàm là `TEMPORARY` → sống trong session hiện tại.
- Mọi hàm: chuỗi rỗng hoặc chữ `"NULL"` → NULL.

| Hàm | Trả về | Ví dụ |
|---|---|---|
| `clean_text(s)` | STRING | `'  a   b '` → `'a b'` |
| `clean_code(s)` | STRING | `' cus 001 '` → `'CUS001'` |
| `to_amount(s)` | DECIMAL(18,2) | `'1,234.50'` → `1234.50` · `'five percent'` → NULL |
| `parse_ts(s)` | TIMESTAMP | `'3/15/2025 9:30'` → `2025-03-15 09:30:00` |
| `parse_date(s)` | DATE | như trên, lấy ngày |

## Làm (kiểm thử)
Trong 1 notebook bất kỳ (đã gắn `lh_platform`):
```
%run nb_common
```
```sql
%%sql
SELECT clean_code(' cus 001 ') c, to_amount('1,234.50') a, to_amount('five percent') bad,
       parse_ts('3/15/2025 9:30') ts1, parse_ts('25/3/2025 14:05:00') ts2, parse_date('NULL') d
```

## Kết quả mong đợi
`CUS001 · 1234.50 · NULL · 2025-03-15 09:30:00 · 2025-03-25 14:05:00 · NULL` ✅ (đã chạy 2026-10-06).
