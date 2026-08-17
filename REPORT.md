# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Ngô Phương Nam **Lớp:** E403 **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 90.6s
  run 2/3 … 73.9s
  run 3/3 … 82.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     7c461563f4    d11657ff21    2b76a4f850   ✗
  gold_feature_daily    4eee63cd82    4eee63cd82    4eee63cd82   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp
```

</details>

Tổng kết: **1 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy


|                       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**     | Kết quả chạy cho thấy`silver_tickets` có đúng  12,480 row tương ứng 12,480 ticket duy nhất , trong khi `gold_training_set` xuất hiện duplicate (một số `ticket_id` lặp 3 lần).                                                                                                                                                                                                                                                                                                                                                                                                      |
| **Nguyên nhân**     | Nguyên nhân nằm ở cách model incremental đang materialize/ghi dữ liệu , cụ thể là chưa cấu hình`unique_key` và `merge`. Source CDC có các bản ghi `op='u'` khi ticket được cập nhật , nên cùng một `ticket_id` có thể xuất hiện lại ở các ngày khác nhau. Khi thiếu `unique_key`, dbt sẽ dùng chiến lược **`append`** cho incremental model, tức là sinh câu lệnh *INSERT* các row mới vào bảng đích. Chạy lại cùng một ngày lần thứ hai, các row cũ  không bị ghi đè mà bị ghi thêm (append) , nên có thể tạo duplicate. |
| **Cách khắc phục** | *file: models/gold/gold_training_set.sql* <br />Bổ sung `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| **Bằng chứng**      | trước: 38750 hàng · sau: 12480 hàng · checksum 3 lượt: stable                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ


|                               |                                                                                                                                                                                                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**             | `gold_feature_daily` chỉ có 8,645 hàng, thiếu các cặp (`event_date`, `customer_id`) ở các ngày quá khứ.                                                                                                                                            |
| **P99 độ trễ đo được** | **2.723 ngày** *(bắt buộc)*                                                                                                                                                                                                                               |
| **Lookback đã chọn**       | 3 ngày — vì P99 ≈ 2.723 ngày, chọn 3 ngày để bao phủ độ trễ đến P99 khi tính theo ngày.                                                                                                                                                    |
| **Nguyên nhân**             | Điều kiện incremental`event_date > max(event_date)` chỉ lấy các ngày mới hơn ngày lớn nhất đã có trong target. Các event đến muộn với `event_date` thuộc ngày quá khứ bị bỏ qua và không được tính vào `gold_feature_daily`. |
| **Cách khắc phục**         | Mở rộng incremental window lùi**3 ngày** và dùng `merge` với `unique_key = ['event_date', 'customer_id']` để các cặp được tính lại có thể **thay thế** bản ghi cũ thay vì bị cộng dồn.                                               |
| **Bằng chứng**              | trước: 8,645 hàng · sau: 9,100 hàng                                                                                                                                                                                                                      |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn **P99** làm căn cứ vì đây là ngưỡng đại diện cho 99% độ trễ ingest, giúp cân bằng giữa khả năng bắt dữ liệu đến muộn và chi phí xử lý. Trong dataset này, P99 = 2.723 ngày và max = 2.945 ngày đều dẫn đến window 3 ngày sau khi lấy ceiling, nên chi phí thực tế hiện tại là như nhau. Tuy nhiên, nếu có outlier lớn hơn nhiều, dùng `max` sẽ buộc phải mở rộng window và làm tăng lượng dữ liệu phải quét, tính toán lại ở  mọi lượt chạy sau này .

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ


|                                                                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**                                               | `silver_tickets.priority` xuất hiện `NULL`, `0`, `5`, `-1` và các giá trị chuỗi từ source, trong khi contract yêu cầu `priority` là số nguyên trong  **1..4** .                                                                                                                                                                                                                                                                                                                                                                                               |
| **Nguyên nhân**                                               | Source CDC thay đổi cách biểu diễn`priority` từ số sang nhãn chữ (`urgent`, `high`, `medium`, `low`). Việc chỉ dùng `try_cast()` vừa biến các nhãn chữ thành `NULL`, vừa cho phép các số ngoài miền `1..4` đi qua.Sửa macro `normalize_priority` bằng `CASE` để chuẩn hóa cả số và nhãn chữ; lọc bản ghi không chuẩn hóa được **trước khi `row_number()`** trong `silver_tickets`; đưa các bản ghi lỗi vào `quarantine_tickets`; bật `contract.enforced = true` và test `accepted_values: [1,2,3,4]` cho `priority`. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Số hợp lệ:** `1, 2, 3, 4` → giữ nguyên. **Nhãn chữ:** `urgent, high, medium, low` → map lần lượt thành `1, 2, 3, 4`. **Giá trị không hợp lệ:** `P1, unknown, 0, 5, -1, '', NULL` → trả về `NULL` và đưa vào `quarantine_tickets`.                                                                                                                                                                                                                                                                                                               |
| **Cách khắc phục**                                           | Sửa macro`normalize_priority` bằng `CASE` để chuẩn hóa cả số và nhãn chữ; lọc bản ghi không chuẩn hóa được **trước khi `row_number()`** trong `silver_tickets`; đưa các bản ghi lỗi vào `quarantine_tickets`; bật `contract.enforced = true` và test `accepted_values: [1,2,3,4]` cho `priority`.                                                                                                                                                                                                                                             |
| **Bằng chứng**                                                | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên giữ dữ liệu raw ở **Bronze** và xử lý lỗi ở  **Silver** , đồng thời đưa bản ghi lỗi vào quarantine. Bronze cần giữ nguyên dữ liệu gốc để phục vụ audit, điều tra và reprocess; nếu chặn ngay ở Bronze thì sẽ mất bằng chứng về dữ liệu source gây lỗi. Không nên để pipeline dừng vì một số lượng nhỏ bản ghi lỗi có thể được cô lập vào quarantine trong khi phần lớn dữ liệu hợp lệ vẫn tiếp tục được xử lý và phục vụ người dùng. Pipeline vẫn cần theo dõi số lượng bản ghi lỗi để phát hiện và xử lý vấn đề ở source.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md


|                       |                     |
| ----------------------- | --------------------- |
| **Bài đã làm**    | A / B / không làm |
| **Nguyên nhân**     |                     |
| **Cách khắc phục** |                     |
| **Bằng chứng**      |                     |

---

## 5 · Tổng kết


| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên                                                                                                                    |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1          | Kiểm tra**grain, natural key và cách incremental model ghi dữ liệu** (`unique_key`, `incremental_strategy`) để đảm bảo model idempotent và không tạo duplicate.                                |
| 2          | Kiểm tra**độ trễ dữ liệu (late-arriving data)** , đặc biệt là phân bố ingest lag và P99, trước khi quyết định incremental/lookback window.                                                |
| 3          | Kiểm tra**schema và data contract của source** , xác định sự thay đổi kiểu/format dữ liệu, cách chuẩn hóa, quarantine và validation trước khi đưa dữ liệu lên các tầng downstream. |
