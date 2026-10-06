# Hướng dẫn triển khai từng bước

> Sổ tay thao tác cho Platform. [PLAN.md](../PLAN.md) trả lời *làm gì & vì sao* ở mức tổng thể; thư mục này trả lời *làm thế nào, click nào, ra kết quả gì*.
> Đọc theo thứ tự. Mỗi file nhỏ = 1 việc làm xong trong 15–45 phút.

## Cách đọc 1 file

Mọi file cùng khuôn:

1. **Mục tiêu** — xong việc này thì có gì.
2. **Hiểu trước khi làm** — khái niệm cần nắm (ngắn). Từ lạ → [khai-niem.md](khai-niem.md).
3. **Làm** — từng bước, từng click.
4. **Kết quả mong đợi** — nhìn thấy gì là đúng.
5. **Lỗi đã gặp** — lỗi thật đã gặp ở dự án này + cách sửa.

## Lộ trình

| Bước | Thư mục | Trạng thái |
|---|---|---|
| 0. Nền | [00-nen/](00-nen/README.md) | ✅ xong |
| 1. Ingest (Source → landing → Bronze) | [01-ingest/](01-ingest/README.md) | ▶ đang làm |
| 2. Khám phá dữ liệu (Q1) | *(viết khi tới)* | |
| 3. Orders end-to-end | *(viết khi tới)* | |
| 4+ | xem [PLAN §15](../PLAN.md#15-roadmap-từng-bước) | |

## 4 thói quen bắt buộc

| Thói quen | Vì sao (đã trả giá) |
|---|---|
| **Pipeline: Ctrl+S sau mỗi activity** | Pipeline **không tự lưu** như notebook; mất trang = mất hết |
| **Xong 1 việc nhỏ → Commit ngay** (Source control → Commit, nhánh `dev`) | Git là bản lưu an toàn duy nhất; có lịch sử để quay lại |
| **Đóng tab notebook trước khi Update all** | Tránh hộp thoại "Saved version / Your version" |
| **1 session Spark mỗi lúc; xong thì Stop session** | Capacity trial nhỏ → lỗi 430 `TooManyRequestsForCapacity` |
