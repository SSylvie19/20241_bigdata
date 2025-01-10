from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import functions as F
import subprocess
import os


spark = SparkSession.builder \
    .appName("Property Processing") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate()


hdfs_file_path = "hdfs://namenode:8020/upload_data/data.csv"

# Gán tên cột nếu DataFrame không có header
colnames = [
    'Transaction_unique_identifier', 'price', 'Date_of_Transfer',
    'postcode', 'Property_Type', 'Old/New',
    'Duration', 'PAON', 'SAON',
    'Street', 'Locality', 'Town/City',
    'District', 'County', 'PPDCategory_Type',
    'Record_Status - monthly_file_only'
]

# Đọc dữ liệu từ HDFS
raw_df = spark.read.option("header", "false").csv(hdfs_file_path)

# Gán tên cột
raw_df = raw_df.toDF(*colnames)


property_type_mapping = {
    'D': 1,  # Detached
    'S': 2,  # Semi-detached
    'T': 3,  # Terraced
    'F': 4,  # Flats/Maisonettes
    'O': 5   # Other
}

duration_mapping = {
    'F': 1, # Freehold (sở hữu vĩnh viễn)
    'L': 2, # Leasehold (Sở hữu có thời hạn)
    'U': 3, # Unregistered (Chưa đăng ký)
}

def preprocess(raw_df):
    # Drop unnecessary columns
    columns_to_drop = [
        'Transaction_unique_identifier', 'Street', 'Town/City',
        'Record_Status - monthly_file_only'
    ]
    df = raw_df.drop(*columns_to_drop)
    
    # Convert 'Date_of_Transfer' column
    df = df.withColumn('Date_of_Transfer', F.to_date(df['Date_of_Transfer']))

    # Map 'Property_Type' column
    property_type_expr = F.when(F.lit(True), F.lit(None).cast(IntegerType()))
    for key, value in property_type_mapping.items():
        property_type_expr = property_type_expr.when(df['Property_Type'] == key, value)
    df = df.withColumn('Property_Type', property_type_expr)

    # Fill missing values for 'SAON' and 'PAON'
    df = df.fillna({'SAON': 0, 'PAON': 0})

    # Fill missing values for 'postcode'
    df = df.fillna({'postcode': 'UNKNOWN'})

    # Convert 'Old/New' column
    df = df.withColumn('Old/New', F.when(df['Old/New'] == 'Y', 1).otherwise(0))

    # Map 'Duration' column
    duration_expr = F.when(F.lit(True), F.lit(None).cast(IntegerType()))
    for key, value in duration_mapping.items():
        duration_expr = duration_expr.when(df['Duration'] == key, value)
    df = df.withColumn('Duration', duration_expr)

    # Convert 'PPDCategory_Type' column
    df = df.withColumn('PPDCategory_Type', F.when(df['PPDCategory_Type'] == 'A', 1).otherwise(0))

    # Fill missing values for all columns that may have NULLs
    df = df.fillna('UNKNOWN')

    return df

# Đọc dữ liệu (giả định rằng raw_df đã được đọc dưới dạng Spark DataFrame)
# raw_df = spark.read.csv("path_to_csv", header=True, inferSchema=True)

# Chạy hàm preprocess
processed_df = preprocess(raw_df)

# Hiển thị kết quả
processed_df.show()
print("Số lượng bản ghi:", processed_df.count())

output_path = "hdfs://namenode:8020/processed_data/process_data.csv"
temp_output_path = "hdfs://namenode:8020/processed_data_temp"


#temp = "hdfs://namenode:8020/temp"
# Lưu tạm thời
processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)
#processed_df.write.mode("overwrite").option("header", "true").csv(temp)


# Tìm tệp CSV trong thư mục tạm thời
hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(temp_output_path)
status = hdfs.listStatus(path)
print(status)

# Di chuyển tệp từ thư mục tạm tới thư mục đích
csv_file = [file.getPath().toString() for file in status if file.getPath().toString().endswith(".csv")][0]
hdfs.rename(spark._jvm.org.apache.hadoop.fs.Path(csv_file), spark._jvm.org.apache.hadoop.fs.Path(output_path))


hdfs.delete(path, True)
