# Lỗi đã gặp — Bước 1

| Lỗi / hiện tượng | Nguyên nhân | Sửa |
|---|---|---|
| Không tìm thấy *SQL analytics endpoint* của `lh_platform` trong OneLake catalog | Pipeline không liệt kê endpoint riêng | Chọn connection **Lakehouse** → Root folder **Tables** → Use query **T-SQL Query** (chạy qua endpoint) |
| `Invalid connection credentials` khi tạo connection SQL Server | Connection tạo ra chưa Sign in | Không cần connection này nữa (`cn_lh_platform_sql`) — xoá ở Settings → Manage connections and gateways |
| `Invalid object name 'meta.cfg_source_entity'` | SQL analytics endpoint chưa đồng bộ bảng mới tạo bằng Spark | Mở `lh_platform` → đổi sang **SQL analytics endpoint** → **Refresh** → đợi ~1 phút |
| Mũi tên nối khối có điều kiện **Skipped** / **Completed** | Kéo nhầm từ ô ↷ (Skipped) hoặc → (Completed) | Xoá mũi tên, kéo lại từ ô **✓ xanh lá** (Succeeded). Kiểm bằng `{ }` → `dependencyConditions` |
| Biểu thức có `\n` ở cuối (`"@utcNow(...)\n"`) | Nhấn Enter khi dán vào ô dynamic content | Xoá dòng trống cuối trong ô |
| `Saving error: 'sw_source_type' should have at least one Activity` | Case của Switch không được để trống | Tạm thêm **Wait** 1 giây vào case trống |
| Canvas trống, mất Parameters sau khi quay lại | Pipeline **không tự lưu**; thay đổi chưa Save bị bỏ | Ctrl+S sau mỗi activity; Commit sớm |
| Bấm Preview data ở Copy → hộp thoại hỏi giá trị `item()…` | Preview cần giá trị thật cho biểu thức | Cancel — kiểm bằng cách Run thật |
| Run xong nhưng Files trống | Explorer chưa làm mới | **…** cạnh Files → **Refresh** |
