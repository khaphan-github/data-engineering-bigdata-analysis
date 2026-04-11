# Business Use Case: Dự đoán tỷ lệ khách hàng rời bỏ (Customer Churn Prediction)

## 1) Bài toán kinh doanh
Doanh nghiệp (telco/subscription/e-commerce) đang mất khách hàng theo tháng, làm giảm doanh thu lặp lại (MRR) và tăng chi phí thu hút khách hàng mới.

Mục tiêu là dự đoán khách hàng có nguy cơ rời bỏ trong 30 ngày tới, từ đó ưu tiên chiến dịch giữ chân đúng người, đúng thời điểm.

## 2) Mục tiêu business chính
- Giảm churn rate theo tháng (ví dụ: từ 5.2% xuống 4.4% trong 2 quý).
- Tăng doanh thu giữ chân (retention revenue).
- Tối ưu chi phí khuyến mại/chăm sóc bằng cách chỉ tác động nhóm có khả năng rời bỏ cao.

## 3) Câu hỏi nghiệp vụ cần trả lời
- Khách hàng nào có khả năng rời bỏ cao trong 30 ngày?
- Vì sao họ có nguy cơ rời bỏ (giá, chất lượng dịch vụ, tần suất sử dụng, support ticket, thanh toán thất bại...)?
- Nếu can thiệp, cần ưu tiên can thiệp ai trước để tối đa ROI?

## 4) Định nghĩa output cho vận hành
- Churn risk score cho từng khách hàng: 0-1.
- Risk tier:
  - High risk: >= 0.8
  - Medium risk: 0.5-0.79
  - Low risk: < 0.5
- Top reason codes (SHAP/feature importance) để team CS/Marketing sử dụng.
- Danh sách action mỗi ngày cho CRM:
  - Gọi chăm sóc
  - Offer ưu đãi giữ chân
  - Gợi ý gói dịch vụ phù hợp

## 5) Nguồn dữ liệu pipeline
- Transaction data: đơn hàng, giá trị, tần suất mua.
- Product usage: tần suất đăng nhập, session, feature usage.
- Billing/payment: trạng thái thanh toán, nợ quá hạn, failed payment.
- Customer support: số ticket, loại ticket, thời gian xử lý, sentiment.
- Customer profile: tenure, gói cước/gói dịch vụ, kênh đăng ký.

## 6) KPI đánh giá thành công
- Model KPI: AUC, Precision@K, Recall@K, calibration.
- Business KPI:
  - Churn rate trước/sau triển khai
  - Retention uplift theo cohort được can thiệp
  - Incremental revenue giữ được
  - Cost per saved customer
  - ROI chiến dịch giữ chân

## 7) Luồng nghiệp vụ end-to-end
1. Thu thập + đồng bộ dữ liệu hằng ngày vào data lake/warehouse.
2. Tạo feature snapshot theo ngày, tạo nhãn churn (rời bỏ trong 30 ngày).
3. Train model định kỳ (tuần/tháng), theo dõi model drift.
4. Score hằng ngày và đẩy kết quả sang CRM.
5. Chạy campaign giữ chân theo risk tier.
6. Đo lường uplift và đóng vòng phản hồi vào lần train tiếp theo.

## 8) Giá trị mang lại
Use case này là bài toán cốt lõi cho hệ thống data pipeline vì kết nối trực tiếp:
- Data engineering (ETL + quality),
- ML engineering (feature store + training + serving),
- Business operations (CRM campaign + đo lường ROI).

Đây là use case chính rất phù hợp để demo hệ thống dữ liệu ngoài thực tế và khả năng vận hành AI trong doanh nghiệp.
