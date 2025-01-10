from hdfs import InsecureClient

# Khởi tạo kết nối với HDFS
hdfs_client = InsecureClient('http://namenode:9870', user='root')  # Cập nhật đúng địa chỉ NameNode

# Đường dẫn tới file trong HDFS
hdfs_file_path = '/processed_data/processed_data.csv'

# Đọc file từ HDFS
with hdfs_client.read(hdfs_file_path, encoding='utf-8') as reader:
    # Đọc nội dung của file
    content = reader.read()
    print(content)