# 0.2 — `nb_setup_ddl`: schema + bảng meta ✅

## Mục tiêu
Có 4 schema `brz`, `slv`, `gld`, `meta` và 13 bảng `meta.*` (rỗng).

## Hiểu trước khi làm
- Notebook chỉ dùng `CREATE … IF NOT EXISTS` → **chạy lại không mất dữ liệu**.
- Bảng `brz/slv/gld` **không** tạo ở đây — notebook của từng bước tự tạo.
- Muốn đổi cấu trúc bảng meta: **thêm cell mới ở cuối** (`ALTER TABLE … ADD COLUMNS`), không sửa cell cũ.
- Cell đầu `%%configure -f` gắn `lh_platform` làm lakehouse mặc định **theo tên** → cùng notebook chạy đúng ở Dev và Prod.

## Làm
1. Đóng mọi tab notebook → Source control → **Update all**.
2. Mở `nb_setup_ddl` → **Run all** (đừng bấm Connect trước — để Run all tự mở session).
3. Xong → nút ⏹ **Stop session**.

## Kết quả mong đợi
- Cell cuối `SHOW TABLES IN meta` → **13 dòng**.
- Kiểm vị trí: `DESCRIBE TABLE EXTENDED meta.cfg_source_entity` → dòng **Location** chứa id của `lh_platform` (`…/79063a99-…/Tables/meta/cfg_source_entity` ở Dev).
- Cột `namespace` hiện `<no-lakehouse…>` là cách Fabric hiển thị — **không phải lỗi**.

## Lỗi đã gặp
Xem [99-loi-da-gap.md](99-loi-da-gap.md): `%%configure` trong high-concurrency session · cần `-f` · lỗi 430.
