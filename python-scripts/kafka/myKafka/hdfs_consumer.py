from kafka import KafkaConsumer
import json
import csv
from hdfs import InsecureClient

# Khởi tạo Kafka consumer
consumer = KafkaConsumer('weather_data', bootstrap_servers='kafka:29092', auto_offset_reset='earliest')

# Khởi tạo HDFS client
hdfs_client = InsecureClient('http://namenode:9870', user='root')  # Cập nhật đúng địa chỉ NameNode

# Đường dẫn tới file CSV trong HDFS
hdfs_file_path = '/user/data.csv'

fieldnames = ["Transaction_unique_identifier", "price", "Date_of_Transfer", "postcode", 
                      "Property_Type", "Old/New", "Duration", "PAON", "SAON", "Street", 
                      "Locality", "Town/City", "District", "County", "PPDCategory_Type", 
                      "Record_Status - monthly_file_only"]

# Kiểm tra xem file có tồn tại trên HDFS không, nếu không thì tạo mới
if not hdfs_client.status(hdfs_file_path, strict=False):
    # Nếu file chưa có trên HDFS, tạo file mới với header
    with hdfs_client.write(hdfs_file_path, overwrite=True, encoding='utf-8') as writer:
        csv_writer = csv.DictWriter(writer, fieldnames=fieldnames)
        csv_writer.writeheader()  # Ghi header vào file
    print("file created")

# Mở kết nối để append dữ liệu vào file trong HDFS
with hdfs_client.write(hdfs_file_path, append=True, encoding='utf-8') as writer:
    csv_writer = csv.DictWriter(writer, fieldnames=fieldnames)

    # Đọc tin nhắn từ Kafka và ghi vào HDFS
    for msg in consumer:
        value = msg.value.decode('utf-8')
        print(value)

        # Chuyển đổi chuỗi JSON thành đối tượng Python
        data = json.loads(value)

        # Ghi dữ liệu vào HDFS
        if isinstance(data, list):
            for item in data:
                csv_writer.writerow(item)
        else:
            csv_writer.writerow(data)
