from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Property Processing") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.index.auto.create", "true") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# File path
file_path = 'hdfs://namenode:8020/process_data/processed_data.csv'
output_file_path = 'hdfs://namenode:8020/process_data/property_type.csv'

# Read entire CSV into a Spark DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Sum by 'Duration' and 'Old/New' (calculate sums for each category)
# We map 'Duration' and 'Old/New' to 'Freehold', 'Leasehold', 'Old', 'New' values

# Map numeric values to their respective string labels
df = df.withColumn('Duration_Label', when(col('Duration') == 1, 'Freehold')
                                     .when(col('Duration') == 2, 'Leasehold')
                                     .otherwise(None))

df = df.withColumn('Old_New_Label', when(col('Old/New') == 0, 'Old')
                                       .when(col('Old/New') == 1, 'New')
                                       .otherwise(None))

# Calculate sum of 'price' for each category ('Duration_Label' and 'Old_New_Label')
duration_price_sum = df.groupBy('Duration_Label').agg({'price': 'sum'}).withColumnRenamed('sum(price)', 'Total_Price')
old_new_price_sum = df.groupBy('Old_New_Label').agg({'price': 'sum'}).withColumnRenamed('sum(price)', 'Total_Price')

# Count the number of records for each category ('Duration_Label' and 'Old_New_Label')
duration_count = df.groupBy('Duration_Label').count().withColumnRenamed('count', 'Total_Records')
old_new_count = df.groupBy('Old_New_Label').count().withColumnRenamed('count', 'Total_Records')

# Join the sums and counts to calculate average values
duration_avg_df = duration_price_sum.join(duration_count, 'Duration_Label').withColumn('Avg_Value', col('Total_Price') / col('Total_Records'))
old_new_avg_df = old_new_price_sum.join(old_new_count, 'Old_New_Label').withColumn('Avg_Value', col('Total_Price') / col('Total_Records'))

# Combine both DataFrames for final result
result_df = duration_avg_df.select('Duration_Label', 'Avg_Value').union(
            old_new_avg_df.select('Old_New_Label', 'Avg_Value'))

# Rename columns for consistency in output
result_df = result_df.withColumnRenamed('Duration_Label', 'Type') \
                     .withColumnRenamed('Old_New_Label', 'Type') \
                     .select('Type', 'Avg_Value')

# Write the result to a CSV file
result_df.write.option("header", "true").csv(output_file_path)

print(f"Summary written to {output_file_path}")
