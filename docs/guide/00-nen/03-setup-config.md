# 0.3 — `nb_setup_config`: ghi config ✅

## Mục tiêu
Các bảng `cfg_*` / `ref_*` có nội dung đúng như trong notebook.

## Hiểu trước khi làm
- **Notebook này là nguồn sự thật của config.** Không sửa tay bảng trên Fabric. Muốn đổi: sửa VALUES trong notebook → Commit → chạy lại.
- Mỗi bảng 1 lệnh `MERGE … USING VALUES`:
  - dòng mới → thêm · dòng có sẵn → cập nhật · **dòng không còn trong script → xoá** (`WHEN NOT MATCHED BY SOURCE THEN DELETE`).
  - → bảng luôn khớp đúng notebook, chạy lại bao nhiêu lần cũng được.
- Nội dung hiện có:

  | Bảng | Dòng | Ý nghĩa |
  |---|---|---|
  | `cfg_source_entity` | 9 | 4 entity ERP (`db`, incremental theo `updated_at`) + 4 entity retail & 1 reference (`file`, theo LastModified) |
  | `ref_order_status` | 4 | Pending · Shipped · Delivered (**tính doanh thu**) · Cancelled |
  | `ref_value_mapping` | 4 | giá trị chuẩn `order_status`; phần còn lại bổ sung ở Bước 2 |

## Làm
1. Mở `nb_setup_config` → **Run all** → xong **Stop session**.

## Kết quả mong đợi
- Cell cuối: `cfg_source_entity = 9`, `ref_order_status = 4`, `ref_value_mapping = 4`.
- Lần đầu mỗi MERGE báo `num_inserted_rows` = số dòng; chạy lại báo `num_updated_rows` (nội dung không đổi) — bình thường.
