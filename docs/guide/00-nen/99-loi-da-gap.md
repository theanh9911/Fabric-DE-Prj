# Lỗi đã gặp — Bước 0

| Lỗi | Nguyên nhân | Sửa |
|---|---|---|
| `MagicUsageError: %%configure is not supported in high concurrency session` | Notebook chạy trong session high-concurrency (`HC_…`) | Workspace settings → Spark settings → High concurrency → tắt "For notebooks"; Stop session → chọn **Standard session** |
| `The current running Livy session must be restarted… Specify "-f"` | Session đã mở trước khi tới cell `%%configure` | Dùng `%%configure -f` (đã sửa trong mọi notebook) |
| `TooManyRequestsForCapacity … HTTP 430` | Hết core Spark của capacity trial: session cũ chưa trả core (trễ 1–2 phút), hoặc nhiều session cùng lúc | Monitoring hub → Cancel session đang chạy · đóng tab notebook thừa · đợi 1–2 phút · **bấm Run all thẳng, không Connect trước** |
| Hộp thoại *Saved version / Your version* sau khi Update all | Tab notebook đang mở bản cũ trong lúc Update | Chọn **Keep this version** ở bên **Saved version** (bản từ Git). Lần sau đóng tab trước khi Update |
| Banner "Upgrade to Runtime 2.0" | Thông báo chung của Fabric | Bỏ qua — ws đã ở Runtime 2.0 |
| Dòng trống trong ô markdown bị Fabric gộp, bảng thành heading | Định dạng file Git của Fabric | Tách nội dung sau dòng trống sang ô markdown mới |
