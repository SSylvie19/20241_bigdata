from hdfs import InsecureClient

# Đường dẫn đến HDFS NameNode và thư mục
hdfs_url = 'http://namenode:9870'  # Thay bằng địa chỉ NameNode của bạn
hdfs_path = '/Processed_data/Processed_data.csv'
local_file = 'processed_data.csv'

# Kết nối với HDFS
client = InsecureClient(hdfs_url, user='root')  # Thay 'hdfs' bằng tên user của bạn

# Upload file lên HDFS
try:
    client.upload(hdfs_path, local_file, overwrite=True)
    print(f"Đã lưu file {local_file} vào HDFS tại {hdfs_path}")
except Exception as e:
    print(f"Lỗi khi lưu file vào HDFS: {e}")
