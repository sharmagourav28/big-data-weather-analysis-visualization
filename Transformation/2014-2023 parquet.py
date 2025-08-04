from pyspark.sql.functions import col, to_date, year

# Load the CSVs (assuming your data is in CSV format and already on disk or cloud)
df = spark.read.option("header", True).csv("s3://wd1public/wd1publicdata/")

# Convert 'date' column to proper DateType (assuming the column is named 'date')
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Filter rows from 2014 to 2023
df_filtered = df.filter((year(col("date")) >= 2014) & (year(col("date")) <= 2023))

df_filtered.repartition(50) \
    .write.mode("overwrite") \
    .parquet("s3://wd2-20142023/wd2_2014_2023/")


from pyspark.sql.functions import col, to_date, year

# Load the CSVs (assuming your data is in CSV format and already on disk or cloud)
df = spark.read.option("header", True).csv("s3://wd3filesgroup7/files/")

# Convert 'date' column to proper DateType (assuming the column is named 'date')
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Filter rows from 2014 to 2023
df_filtered = df.filter((year(col("date")) >= 2014) & (year(col("date")) <= 2023))

df_filtered.repartition(50) \
    .write.mode("overwrite") \
    .parquet("s3://wd2-20142023/wd3_2014_2023/")




from pyspark.sql.functions import col, to_date, year

# Load the CSVs (assuming your data is in CSV format and already on disk or cloud)
df = spark.read.option("header", True).csv("s3://wd1public/wd1publicdata/")

# Convert 'date' column to proper DateType (assuming the column is named 'date')
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Filter rows from 2014 to 2023
df_filtered = df.filter((year(col("date")) >= 2014) & (year(col("date")) <= 2023))

df_filtered.repartition(50) \
    .write.mode("overwrite") \
    .parquet("s3://wd2-20142023/wd1_2014_2023/")
