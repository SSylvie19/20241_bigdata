from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, sum, count

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Property Processing") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Đọc dữ liệu CSV với Spark
file_path = 'hdfs://namenode:8020/process_data/processed_data.csv'
df = spark.read.option("header", "true").csv(file_path)

# Đảm bảo rằng cột 'Date_of_Transfer' được chuyển sang định dạng ngày tháng và 'Year' được tạo
df = df.withColumn('Date_of_Transfer', col('Date_of_Transfer').cast('timestamp')) \
       .withColumn('Year', year(col('Date_of_Transfer')))

# Group theo 'Year' để tính tổng giá và số lượng giao dịch
grouped_df = df.groupBy('Year').agg(
    sum('price').alias('sum'),
    count('price').alias('count')
)

# Tính giá trị trung bình
result_df = grouped_df.withColumn('Avg_Value', col('sum') / col('count'))

# Chọn các cột cần thiết và sắp xếp theo 'Year'
result_df = result_df.select('Year', 'count', 'Avg_Value').orderBy('Year')

# Chuyển đổi kết quả thành DataFrame và ghi ra CSV
output_file_path = 'hdfs://namenode:8020/process_data/property_data_10_years.csv'

# Giới hạn kết quả cho 10 năm đầu tiên
result_df = result_df.limit(10)

# Ghi kết quả vào CSV
result_df.coalesce(1).write.option("header", "true").csv(output_file_path)

# Dừng Spark session khi xong
spark.stop()
