from kafka import KafkaConsumer
import json
import csv



consumer = KafkaConsumer('weather_data', bootstrap_servers='kafka:29092', auto_offset_reset='latest')


with open("data.csv", mode="a", newline="", buffering=1) as csv_file:

    # Định nghĩa các trường (fieldnames) dựa trên dữ liệu JSON
    # Giả sử các khóa trong JSON là các trường dữ liệu cần ghi vào CSV
    fieldnames = ["Transaction_unique_identifier", "price", "Date_of_Transfer", "postcode", 
                  "Property_Type", "Old/New", "Duration", "PAON", "SAON", "Street", 
                  "Locality", "Town/City", "District", "County", "PPDCategory_Type", 
                  "Record_Status - monthly_file_only"]
    
    # Khởi tạo csv.DictWriter để ghi dữ liệu vào CSV
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Kiểm tra xem file đã có dữ liệu hay chưa, nếu không thì ghi header
    csv_file.seek(0, 2)
    if csv_file.tell() == 0:
        writer.writeheader()

    # Giả sử consumer là đối tượng Kafka Consumer đã được khởi tạo và đang lắng nghe tin nhắn
    for msg in consumer:
        # Giải mã giá trị tin nhắn từ bytes thành string
        value = msg.value.decode('utf-8')
        print(value)
        
        # Chuyển đổi chuỗi JSON thành đối tượng Python (danh sách hoặc từ điển)
        data = json.loads(value)

        # Ghi dữ liệu vào file CSV
        # Nếu dữ liệu là một danh sách các đối tượng, bạn cần lặp qua từng đối tượng trong danh sách
        if isinstance(data, list):
            for item in data:
                writer.writerow(item)
        else:
            # Nếu dữ liệu chỉ có một đối tượng, ghi trực tiếp vào CSV
            writer.writerow(data)
