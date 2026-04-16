1. Bối cảnh: Một hệ thống thương mại điện tử được thiết kế theo kiến trúc microservice
   - Gồm 3 service chính là orders, users và products: Mỗi service có 1 team riêng và data databse riêng để lưu trữ và xử lý nghiệp vụ.
   - Vì 3 service phục vụ các mục đích riêng nên: phía order, user đang dùng PostgreSQL , phía product đang dùng MongoDB.
   - Hằng ngày, các service sẽ sinh ra hàng triệu sự kiện giao dịch và thanh toán, được ghi nhận và lưu trữ trong các database của từng service.
   - Hệ thống có các cơ chế backup dữ liệu chạy hằng đêm để đảm bảo dữ liệu khong bị mất.

2. Vấn đề: Để tối ưu hiệu quả kinh doanh các team cần có các thông tin khác nhau để tổng hợp và phân tích dữ liệu nhằm đưa ra chiến lược phù hợp cho công ty:

- CEO:
  - Nhóm này cần biết các thông tin:
    - Doanh thu theo ngày, tuần, tháng
    - Số lượng đơn hàng và khách hàng mới theo thời gian
    - Tỷ lệ chuyển đổi từ khách truy cập thành khách hàng
    - Giá trị đơn hàng trung bình (AOV)
    - Tỷ lệ churn của khách hàng
- Team marketing:
  - Nhóm này cần biết các thông tin:
    - Hiệu quả của các chiến dịch quảng cáo (ROI, CPA)
    - Phân khúc khách hàng theo hành vi mua sắm và nhân khẩu học
- Team phát triển sản phẩm:
  - Nhóm này cần biết các thông tin:
    - Tỷ lệ giữ chân khách hàng (retention rate)

3. Để giải quyết nhu cầu này team Dev cần xây dựng một hệ thống cỏ khả năng:

- Thu thập dữ liệu từ các nguồn khác nhau (PostgreSQL, MongoDB) một cách hiệu quả và đáng tin cậy.
- Xử lý và biến đổi dữ liệu để tạo ra các bảng phân tích phù hợp với nhu cầu của từng nhóm người dùng.
- Cung cấp các công cụ và giao diện để người dùng cuối có thể truy cập và phân tích dữ liệu một cách dễ dàng.
- Dữ liệu cần được cập nhật chính xac và kịp thời để đảm bảo các quyết định kinh doanh được dữ liệu hỗ trợ tốt nhất.
- Khả năng chịu lỗi và mở rộng phải cao đồng thời phải linh hoạt để đảm bảo hệ thống adapt được sự thay đỗi nhanh chóng của bussiness.

4. Bối cảnh doanh nghiệp đang trong giai đoạn chuyển giao hạ tần từ onpremis -> cloud.

# English version

- Context: A microservices-based e-commerce system with separate databases for orders, users, and products. The system generates millions of transactions daily, and different teams (CEO, marketing, product) require various insights for decision-making. The data architecture must support efficient data collection, processing, and analysis while being scalable and adaptable to business changes. The company is transitioning from on-premises to cloud infrastructure.
