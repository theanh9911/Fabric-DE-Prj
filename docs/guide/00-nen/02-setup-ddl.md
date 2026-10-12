# 0.2 — `nb_setup_ddl`: schema + bảng meta (lịch sử)

> Tài liệu lịch sử Bước 0.2; trạng thái DDL hiện hành và migration xem ở [Bước 1.2](../01-ingest/01-meta-setup.md).

## Mục tiêu cũ
Bước 0 từng tạo 4 schema và bộ bảng meta v2. Hướng dẫn hiện hành là Bước 1.2–1.4; không chạy các bước/câu lệnh bên dưới.

## Hiểu trước khi làm
- Các ghi chú dưới đây mô tả notebook v2; DDL/migration hiện hành có thể khác và phải theo guide Bước 1.
- Bảng `brz/slv/gld` **không** tạo ở đây — notebook của từng bước tự tạo.
- Muốn đổi cấu trúc bảng meta: **thêm cell mới ở cuối** (`ALTER TABLE … ADD COLUMNS`), không sửa cell cũ.
- Ghi chú lịch sử: notebook v2 từng thử `%%configure -f` để gắn Lakehouse. Notebook hiện hành không dùng magic này; gắn `lh_platform` qua **Data items** và default Lakehouse binding theo runbook hiện hành.

## Làm
1. Đóng mọi tab notebook → Source control → **Update all**.
2. Mở `nb_setup_ddl` → **Run all** (đừng bấm Connect trước — để Run all tự mở session).
3. Xong → nút ⏹ **Stop session**.

## Kết quả mong đợi
- Kết quả theo phiên bản hiện hành: xem Bước 1.2 (3 bảng sau migration).
- Kiểm vị trí: `DESCRIBE TABLE EXTENDED meta.cfg_source_entity` → dòng **Location** chứa id của `lh_platform` (`…/79063a99-…/Tables/meta/cfg_source_entity` ở Dev).
- Cột `namespace` hiện `<no-lakehouse…>` là cách Fabric hiển thị — **không phải lỗi**.

## Lỗi đã gặp
Xem [99-loi-da-gap.md](99-loi-da-gap.md) để phân biệt lỗi binding với lỗi Spark capacity. Không thêm lại `%%configure -f` vào notebook hiện hành.
