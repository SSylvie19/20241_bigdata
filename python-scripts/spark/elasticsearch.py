import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
 
# Kết nối Elasticsearch
ELASTICSEARCH_HOST = "http://localhost:9200"
INDEX_NAME = "real_estate_data"
 
def create_index(es, index_name):
    """Tạo index nếu chưa tồn tại"""
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
                        "price": {"type": "integer"},
                        "Date_of_Transfer": {"type": "date", "format": "MM/dd/yyyy"},
                        "postcode": {"type": "keyword"},
                        "Property_Type": {"type": "integer"},
                        "Old/New": {"type": "integer"},
                        "Duration": {"type": "integer"},
                        "PAON": {"type": "integer"},
                        "SAON": {"type": "integer"},
                        "Locality": {"type": "text"},
                        "District": {"type": "text"},
                        "County": {"type": "text"},
                        "PPDCategory_Type": {"type": "integer"}
                    }
                }
            }
        )
        print(f"Index '{index_name}' created.")
    else:
        print(f"Index '{index_name}' already exists.")
 
def csv_to_elasticsearch(file_path, es, index_name):
    """Đọc file CSV và lưu vào Elasticsearch"""
    df = pd.read_csv(file_path)
 
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
        bulk(es, generate_data())
        print(f"Data successfully indexed into '{index_name}'.")
    except Exception as e:
        print(f"Failed to index data: {e}")
 
if __name__ == "__main__":
    es = Elasticsearch([ELASTICSEARCH_HOST])
    create_index(es, INDEX_NAME)
    FILE_PATH = "path_to_your_csv_file.csv"  # Thay bằng đường dẫn file CSV
    csv_to_elasticsearch(FILE_PATH, es, INDEX_NAME)