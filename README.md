# ds200-lab4-22521207
22521207
# Laptop Price Streaming Prediction with Spark Structured Streaming

## Mô tả dự án

Dự án sử dụng Apache Spark Structured Streaming để xây dựng mô hình dự đoán giá laptop dựa trên các đặc trưng như tốc độ CPU, RAM, dung lượng lưu trữ, kích thước màn hình và trọng lượng. 

Quá trình gồm hai phần chính:
- **Phần 1:** Đọc dữ liệu lịch sử từ file CSV để huấn luyện mô hình dự đoán ban đầu.
- **Phần 2:** Thiết lập Spark Structured Streaming đọc dữ liệu đầu vào dạng stream từ một server socket, xử lý dữ liệu theo batch, và áp dụng mô hình đã huấn luyện để dự đoán giá trong thời gian thực.

---

## Hướng dẫn sử dụng

### 1. Huấn luyện mô hình ban đầu với dữ liệu lịch sử

- Dữ liệu lịch sử về giá laptop nằm trong file `Laptop_price.csv`.
- Mô hình Linear Regression được huấn luyện trên dữ liệu này với các bước tiền xử lý:
  - Imputation các giá trị thiếu với giá trị trung bình
  - Chuẩn hóa dữ liệu
  - Tạo pipeline để huấn luyện mô hình.
- Sau khi huấn luyện, mô hình được lưu giữ trong biến toàn cục `pipeline_model_global`.

### 2. Thiết lập streaming nhận dữ liệu và dự đoán

- Spark đọc dữ liệu streaming từ socket TCP (host: `localhost`, port: `9999`).
- Dữ liệu stream gửi đến dạng chuỗi CSV gồm các đặc trưng laptop.
- Mỗi batch dữ liệu được xử lý thông qua hàm `process_batch`:
  - In thông tin schema, xem một vài dòng dữ liệu đầu vào.
  - Áp dụng mô hình đã huấn luyện để dự đoán giá laptop.
  - Hiển thị kết quả dự đoán.
- Streaming thực hiện trigger mỗi 15 giây.

### 3. Server Python gửi dữ liệu

- Server Python đọc file `Laptop_price.csv` và gửi từng dòng dữ liệu qua socket TCP tới Spark Streaming.
- Server lắng nghe kết nối từ Spark, sau khi kết nối được thiết lập sẽ gửi lần lượt từng dòng dữ liệu.
- Có delay 0.2 giây giữa các lần gửi để mô phỏng stream dữ liệu.

---

## Cấu trúc các file chính

- `streaming_laptop_price.py`: Script chính dùng Spark Structured Streaming để đọc stream và dự đoán giá.
- `data_streamer.py`: Server Python dùng để gửi dữ liệu từ CSV qua socket cho Spark đọc.

---

## Yêu cầu môi trường

- Apache Spark (phiên bản hỗ trợ Structured Streaming)
- Python 3.x
- PySpark
- pandas (cho phần server gửi dữ liệu)

---

## Cách chạy

1. Chạy file stream:

python stream.py

2. Chạy file main:
   
spark-submit main.py
