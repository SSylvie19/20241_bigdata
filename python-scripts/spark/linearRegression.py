from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import DoubleType

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Airlines Data Processing") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Đọc dữ liệu từ HDFS
file_path = "hdfs://namenode:8020/process_data/processed_data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Chọn các cột cần thiết và loại bỏ các cột không cần thiết
columns_to_keep = ['price', 'postcode', 'Property_Type', 'Old/New',
                   'Duration', 'PPDCategory_Type']
df_selected = df.select(columns_to_keep)

# Chuyển đổi dữ liệu cần thiết thành số (nếu có cột dạng chuỗi)
from pyspark.ml.feature import StringIndexer

categorical_cols = ['postcode', 'Property_Type', 'Old/New', 'Duration', 'PPDCategory_Type']
for col in categorical_cols:
    indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index")
    df_selected = indexer.fit(df_selected).transform(df_selected)

# Lựa chọn cột đặc trưng (features) và nhãn (label)
feature_cols = [f"{col}_index" for col in categorical_cols]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df_selected).select("features", df_selected['price'].cast(DoubleType()).alias("label"))

# Chia dữ liệu thành tập huấn luyện và kiểm tra
train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)

# Khởi tạo và huấn luyện Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_data)

# Lưu mô hình vào HDFS
model_path = "hdfs://namenode:8020/models/linear_regression_model"
lr_model.save(model_path)
print(f"Model saved to {model_path}")

# Tải lại mô hình từ HDFS (khi cần)
from pyspark.ml.regression import LinearRegressionModel
loaded_model = LinearRegressionModel.load(model_path)
print("Model loaded successfully.")

# Đánh giá mô hình đã tải trên tập kiểm tra
test_results = loaded_model.evaluate(test_data)
print(f"R2 Score: {test_results.r2}")
print(f"RMSE: {test_results.rootMeanSquaredError}")

# Kết thúc SparkSession
spark.stop()