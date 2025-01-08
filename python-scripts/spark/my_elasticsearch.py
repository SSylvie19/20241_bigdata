import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from hdfs import InsecureClient
from elasticsearch.helpers import bulk, BulkIndexError

# Kết nối Elasticsearch
ELASTICSEARCH_HOST = "http://elasticsearch:9200"
INDEX_NAME = "real_estate_data"

# Kết nối HDFS
HDFS_URL = "http://namenode:9870"  # Thay bằng URL HDFS của bạn
HDFS_USER = "root"  # Thay bằng user HDFS của bạn
HDFS_FILE_PATH = "/user/data.csv"  # Đường dẫn file CSV trên HDFS

def create_index(es, index_name):
    """Tạo index nếu chưa tồn tại"""
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 1
                    },
                    "mappings": {
                        "properties": {
                            "id": {"type": "keyword"},  # UUID
                            "price": {"type": "integer"},  # Giá trị số
                            "Date_of_Transfer": {"type": "date", "format": "yyyy-MM-dd"},  # Ngày tháng
                            "postcode": {"type": "keyword"},  # Mã số
                            "Property_Type": {"type": "keyword"},  # Other
                            "Old/New": {"type": "keyword"},  # Ký tự N
                            "Duration": {"type": "keyword"},  # Freehold
                            "PAON": {"type": "integer"},  # Giá trị số
                            "SAON": {"type": "keyword"},  # Có thể trống
                            "Locality": {"type": "text"},  # Jackson Branch
                            "District": {"type": "text"},  # Chester
                            "County": {"type": "text"},  # South Christianstad
                            "Region": {"type": "text"},  # Port Adrienneborough
                            "Country": {"type": "text"},  # Oklahoma
                            "PPDCategory_Type": {"type": "keyword"},  # B
                            "Additional_Info": {"type": "keyword"}  # A
                        }
                    }
                }
            )
            print(f"Index '{index_name}' created.")
        else:
            print(f"Index '{index_name}' already exists.")
    except Exception as e:
        print(f"Error creating index: {e}")

def csv_to_elasticsearch(file_path, es, index_name):
    """Đọc file CSV và lưu vào Elasticsearch"""
    with hdfs_client.read(HDFS_FILE_PATH, encoding='utf-8') as reader:
    # Đọc nội dung của file
        df = pd.read_csv(reader)
 
    # Chuẩn bị dữ liệu
    def generate_data():
        for _, row in df.iterrows():
            yield {
                "_index": index_name,
                "_source": {
                    "price": row["price"],
                    "Date_of_Transfer": row["Date_of_Transfer"],
                    "postcode": row["postcode"],
                    "Property_Type": row["Property_Type"],
                    "Old/New": row["Old/New"],
                    "Duration": row["Duration"],
                    "PAON": row["PAON"],
                    "SAON": row["SAON"],
                    "Locality": row["Locality"],
                    "District": row["District"],
                    "County": row["County"],
                    "PPDCategory_Type": row["PPDCategory_Type"]
                }
            }
 
    # Gửi dữ liệu
    try:
        bulk(es, generate_data(), chunk_size=500)
        print(f"Data successfully indexed into '{index_name}'.")
    except BulkIndexError as e:
        print(f"Failed to index data: {e.errors}")

if __name__ == "__main__":
    # Kết nối Elasticsearch
    es = Elasticsearch([ELASTICSEARCH_HOST], request_timeout = 60)

    # Kết nối HDFS
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

    # Tạo index nếu chưa tồn tại
    create_index(es, INDEX_NAME)

    # Đọc dữ liệu từ HDFS và nhập vào Elasticsearch
    while True:
        csv_to_elasticsearch(HDFS_FILE_PATH, es, INDEX_NAME)
