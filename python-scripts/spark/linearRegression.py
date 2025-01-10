from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
 
# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Airlines Data Processing with Random Forest") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
 
# Đường dẫn file HDFS
file_path = "hdfs://namenode:8020/Processed_data/Processed_data.csv"
 
# Tên các cột
colnames = [
    'price', 'Date_of_Transfer',
    'postcode', 'Property_Type', 'Old/New',
    'Duration', 'PAON', 'SAON', 'Locality',
    'District', 'County', 'PPDCategory_Type',
]
 
# Đọc dữ liệu từ HDFS (không có header)
raw_df = spark.read.option("header", "false").csv(file_path).limit(1000000)
 
# Gán tên cột
df = raw_df.toDF(*colnames)
 
# Chuyển đổi cột 'price' thành kiểu số
df = df.withColumn("price", col("price").cast(DoubleType()))
 
# In schema để kiểm tra tên cột
df.printSchema()
 
# Chọn các cột cần thiết
columns_to_keep = ['price', 'Property_Type', 'Old/New', 'Duration', 'PPDCategory_Type']
df_selected = df.select(columns_to_keep)
 
# Chuyển đổi dữ liệu cần thiết thành số
categorical_cols = ['Property_Type', 'Old/New', 'Duration', 'PPDCategory_Type']
for col in categorical_cols:
    indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="skip")
    df_selected = indexer.fit(df_selected).transform(df_selected)
 
# Lựa chọn cột đặc trưng (features) và nhãn (label)
feature_cols = [f"{col}_index" for col in categorical_cols]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_features = assembler.transform(df_selected).select("features", df_selected['price'].cast(DoubleType()).alias("label"))
 
# Chia dữ liệu thành tập huấn luyện và kiểm tra
train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)
 
# Huấn luyện mô hình Linear Regression
lr = LinearRegression(featuresCol="features", labelCol="label", regParam=0.1)
lr_model = lr.fit(train_data)
 
# Huấn luyện mô hình Random Forest với maxBins tăng lên
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=50, maxDepth=10, maxBins=1000, seed=42)
rf_model = rf.fit(train_data)
 
# Huấn luyện mô hình Decision Tree
dt = DecisionTreeRegressor(featuresCol="features", labelCol="label", maxDepth=10, maxBins=1000, seed=42)
dt_model = dt.fit(train_data)
 
# Đánh giá Linear Regression với MSE và MAE
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction")
print("Linear Regression Evaluation:")
lr_predictions = lr_model.transform(test_data)
lr_mse = evaluator.setMetricName("mse").evaluate(lr_predictions)
lr_mae = evaluator.setMetricName("mae").evaluate(lr_predictions)
print(f"  MSE: {lr_mse}")
print(f"  MAE: {lr_mae}")
 
# Đánh giá Random Forest với MSE và MAE
print("Random Forest Evaluation:")
rf_predictions = rf_model.transform(test_data)
rf_mse = evaluator.setMetricName("mse").evaluate(rf_predictions)
rf_mae = evaluator.setMetricName("mae").evaluate(rf_predictions)
print(f"  MSE: {rf_mse}")
print(f"  MAE: {rf_mae}")


# Đánh giá Decision Tree với MSE và MAE
print("Decision Tree Evaluation:")
dt_predictions = dt_model.transform(test_data)
dt_mse = evaluator.setMetricName("mse").evaluate(dt_predictions)
dt_mae = evaluator.setMetricName("mae").evaluate(dt_predictions)
print(f"  MSE: {dt_mse}")
print(f"  MAE: {dt_mae}")
 
# Lưu cả hai mô hình vào HDFS
lr_model_path = "hdfs://namenode:8020/models/linear_regression_model"
rf_model_path = "hdfs://namenode:8020/models/random_forest_model"
dt_model_path = "hdfs://namenode:8020/models/decision_tree_model"
 


 
 
# Lưu mô hình Linear Regression
lr_model.write().overwrite().save(lr_model_path)
 
# Lưu mô hình Random Forest
rf_model.write().overwrite().save(rf_model_path)
 
# Lưu mô hình Decision Tree vào HDFS
dt_model.write().overwrite().save(dt_model_path)
 
print(f"Linear Regression model saved to {lr_model_path}")
print(f"Random Forest model saved to {rf_model_path}")
print(f"Decision Tree model saved to {dt_model_path}")
 
 
 
 

 
 
 
# Kết thúc SparkSession
spark.stop()
 