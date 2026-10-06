# Hướng dẫn thao tác

> **Đọc theo thứ tự:** [PLAN.md](../PLAN.md) (mục tiêu, roadmap) → [design/](../design/README.md) (thiết kế từng phần — vì sao) → **guide/** (làm thế nào, click nào, ra kết quả gì).
> Thiết kế thay đổi thì sửa **design/** trước, guide cập nhật theo.

## Đọc trước khi làm

| File | Nội dung |
|---|---|
| [khai-niem.md](khai-niem.md) | Từ điển thuật ngữ (landing, watermark, batch, rerun…) |
| [source.md](source.md) | Source là gì, dữ liệu đi từ CSV gốc tới nguồn ra sao, virtual clock |
| [source-bang-cot.md](source-bang-cot.md) | Từng bảng, từng cột của Source |

## Khuôn mỗi file thao tác

1. **Mục tiêu** — xong thì có gì.
2. **Hiểu trước khi làm** — ngắn; chi tiết ở design/.
3. **Làm** — từng bước, từng click. Ai làm: **C** = Claude (code trong repo), **B** = Bạn (Fabric UI).
4. **Kết quả mong đợi** — thấy gì là đúng.
5. **Lỗi đã gặp** — ở file `99-loi-da-gap.md` của từng bước.

## Lộ trình (khớp [PLAN §5](../PLAN.md#5-roadmap-end-to-end))

| Bước | Thư mục | Trạng thái |
|---|---|---|
| 0. Nền | [00-nen/](00-nen/README.md) | ✅ (một phần được thay ở Bước 1) |
| 1. Ingest happy path | [01-ingest/](01-ingest/README.md) | ▶ |
| 2. Source contract + EDA (Q1) | *viết khi tới* | |
| 3. Orders end-to-end → `v0.1` | | |
| 4–12 | xem PLAN §5 | |

## 5 thói quen bắt buộc

| Thói quen | Vì sao (đã trả giá) |
|---|---|
| **Pipeline: Ctrl+S sau mỗi activity** | Pipeline không tự lưu; đã mất trọn pipeline 1 lần |
| **Xong 1 việc nhỏ → Commit ngay** | Git là bản lưu an toàn duy nhất |
| **Đóng tab notebook trước khi Update all** | Tránh hộp thoại "Saved version / Your version" |
| **1 phiên Spark mỗi lúc; xong thì Stop session** | Capacity trial nhỏ → lỗi 430 |
| **Không sửa tay bảng `meta` trên Fabric** | Config sửa trong `nb_setup_config`; state/log do pipeline ghi |
