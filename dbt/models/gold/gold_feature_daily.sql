-- ---------------------------------------------------------------------------
-- gold_feature_daily — đặc trưng theo ngày cho agent định tuyến.
-- Grain: 1 hàng / 1 cặp (event_date, customer_id).
-- ---------------------------------------------------------------------------
-- KHUNG THỰC HIỆN — NHIỆM VỤ 2
--
--   Mỗi ngày vận hành, model chỉ tính lại phần "mới". Định nghĩa "mới" nằm ở
--   khối is_incremental() bên dưới:
--
--       WHERE event_date <toán tử> (
--                 SELECT max(event_date) FROM <bảng đích>
--             ) <lùi lại bao nhiêu ngày?>
--
--   Câu hỏi cần trả lời trước khi sửa:
--     1. Đo phân bố của (_ingested_at - event_time) trong bronze_events.
--        P99 bằng bao nhiêu? Bao nhiêu phần trăm bản ghi tới kho muộn hơn
--        một ngày so với lúc sự kiện xảy ra?
--          Độ trễ được đo bằng _ingested_at - event_time. Cần tính P99 của độ 
--          trễ này và tỷ lệ bản ghi có thời gian ingest muộn hơn 1 ngày so với
--          thời điểm event xảy ra. P99 được dùng để xác định lookback window phù hợp.
--     2. Một bản ghi có event_date = 08-12 nhưng _ingested_at = 08-15: hôm
--        08-15, max(event_date) trong bảng đích đang là bao nhiêu? Bản ghi
--        đó có thoả điều kiện lọc hiện tại không? Ngày hôm sau thì sao?
--          Với bản ghi có event_date = 08-12 nhưng _ingested_at = 08-15, nếu ngày 
--          08-15 max(event_date) trong target là 08-14 thì điều kiện event_date > max(event_date) 
--          cho kết quả FALSE, nên bản ghi bị bỏ qua. Đến ngày 08-16, max(event_date) còn lớn hơn
--          nên bản ghi này vẫn bị bỏ qua.
--     3. Window tính lại nên lùi bao nhiêu ngày? Căn cứ vào P99 hay vào max?
--        Mỗi ngày lùi thêm phải trả giá gì ở MỌI lượt chạy sau này?
--          Window tính lại nên dựa vào P99 của độ trễ _ingested_at - event_time, 
--          không nên dùng max vì max có thể bị ảnh hưởng bởi outlier rất muộn. 
--          Mỗi ngày lùi thêm một ngày sẽ làm tăng lượng dữ liệu phải quét và tính lại, 
--          nên chi phí này phát sinh ở mọi lượt chạy sau đó.
--     4. Khi window mở rộng, cùng một (event_date, customer_id) sẽ được tính
--        lại nhiều lần. Cần thêm gì vào config() để lần tính sau THAY THẾ
--        lần tính trước thay vì cộng dồn? Grain này có mấy cột khoá?
--          Khi mở rộng window, cần thêm unique_key và incremental_strategy = 'merge' 
--          vào config() để lần tính sau thay thế bản ghi trước thay vì cộng dồn. 
--          Grain là (event_date, customer_id), nên có 2 cột khóa:
--          unique_key = ['event_date', 'customer_id'],
--          incremental_strategy = 'merge'
--   Cảnh báo: sửa điều kiện lọc mà không xử lý ý 4 sẽ làm bảng mất tính ổn
--   định — make verify in riêng hai cột "ỔN ĐỊNH" và "SỐ HÀNG" để bạn thấy
--   rõ hai vấn đề này tách nhau.
-- ---------------------------------------------------------------------------

{{ config(
    materialized     = 'incremental',
    unique_key       = ['event_date', 'customer_id'],
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
where event_date >= (select max(event_date) - interval 3 day from {{ this }})
{% endif %}

group by 1, 2, 3, 4
