-- ---------------------------------------------------------------------------
-- gold_training_set — tập huấn luyện cho mô hình phân loại ticket.
-- Grain: 1 hàng / 1 ticket.
-- ---------------------------------------------------------------------------
-- KHUNG THỰC HIỆN — NHIỆM VỤ 1
--
--   Một model incremental của dbt được quyết định bởi ba tham số trong
--   config(). Hiện tại chỉ có một tham số được khai báo:
--
--       materialized         = 'incremental'
--       unique_key           = <chưa khai>
--       incremental_strategy = <chưa khai>
--
--   Câu hỏi cần trả lời trước khi sửa:
--     1. Grain của bảng này là entity hay sự kiện? Khoá tự nhiên là gì?
--      Grain của gold_training_set là entity, với 1 hàng / 1 ticket. 
--      Natural key là ticket_id.
--     2. Khi không có unique_key, dbt sinh ra câu lệnh ghi nào? Chạy lại
--        cùng một ngày lần thứ hai thì hàng cũ bị THAY THẾ hay bị GHI THÊM?
--      Khi không có unique_key, dbt sinh ra câu lệnh ghi kiểu 'append'. Chạy lại
--      cùng một ngày lần thứ hai thì hàng cũ bị GHI THÊM, không bị THAY THẾ.
--     3. Nguồn CDC có bản ghi op='u'. Một ticket được tạo ngày D1 và bị sửa
--        ngày D2 sẽ đi qua mệnh đề WHERE bên dưới mấy lần trong MỘT lượt chạy?
--      Ticket tạo ngày D1 và update ngày D2 sẽ đi qua mệnh đề WHERE 2 lần ở 2 
--      lượt chạy khác nhau: một lần khi run_date = D1 và một lần khi run_date = D2. 
--      Trong mỗi lượt chạy, WHERE chỉ lấy dữ liệu của ngày tương ứng.
--     4. Với dữ liệu như vậy, chiến lược nào phù hợp: 'append',
--        'delete+insert' theo partition ngày, hay 'merge' theo khoá?
--      Chiến lược phù hợp là merge theo ticket_id, vì dữ liệu là entity có CDC op='u'. Cấu hình:
--          - unique_key = 'ticket_id',
--          - incremental_strategy = 'merge'
--      append gây duplicate, còn delete+insert theo partition ngày không phù hợp bằng merge vì một 
--      ticket có thể được cập nhật ở ngày khác với ngày tạo.
--   Lưu ý: mệnh đề WHERE theo run_date bên dưới KHÔNG phải lỗi. Nó tồn tại để
--   backfill một ngày không phải quét lại toàn bộ lịch sử. Giữ nguyên nó.
-- ---------------------------------------------------------------------------

{{ config(
    materialized     = 'incremental',
    unique_key       = 'ticket_id',
    incremental_strategy = 'merge', 
    on_schema_change = 'fail'
) }}

select
    ticket_id,
    customer_id,
    customer_name,
    segment,
    priority,
    category,
    channel,
    status,
    csat,
    first_response_sec,
    length(subject) + length(body)                            as text_len,
    case when status in ('resolved', 'closed') then 1 else 0 end as label_resolved,
    updated_at,
    _ingested_at
from {{ ref('silver_tickets') }}

{% if is_incremental() %}
-- Chỉ xử lý partition của ngày vận hành hiện tại.
where _ingested_at >= TIMESTAMP '{{ var("run_date") }} 00:00:00'
  and _ingested_at <  TIMESTAMP '{{ var("run_date") }} 00:00:00' + interval 1 day
{% endif %}
