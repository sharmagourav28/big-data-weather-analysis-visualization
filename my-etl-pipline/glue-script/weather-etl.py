import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    isnull,
    isnan,
    lit,
    hour,
    to_timestamp,
    to_date,
    when,
    concat_ws,
    avg,
    regexp_replace,
    initcap,
)

# ------------------- Glue & Spark Setup -------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------- Paths -------------------
base_paths = [
    "s3://allweatherrdatasetstatelatlongwd1/wd1data/",
    "s3://allweatherstatelatlongwd2/wd2statedata/",
    "s3://allweatherdatastatelaglongwd3/files/",
]

mapping_path = "s3://citystatelatlong/city_state_latlong/merged_city_state_lat_long.csv"
output_path = "s3://fullautomatedbucket/cleaned_data/"

# ------------------- Load Weather Data -------------------
df_all = spark.read.option("header", True).parquet(*base_paths)


# Extract city from file path
df_cleaned = df_all.withColumn(
    "city", regexp_extract(col("file_only"), r"/([^/]+?)(?:_\d+)?\.csv$", 1)
)

df_with_city = (
    df_cleaned.withColumn("city", regexp_replace("city", "%20", " "))
    .withColumn("city", initcap("city"))
    .withColumn("city", regexp_replace("city", " ", ""))
)  # Remove spaces

# Fill specific null values
df_filled = df_with_city.fillna({"snow_depth": 0})

# ------------------- City Classification -------------------
city_file_counts = df_with_city.groupBy("city").agg(
    F.countDistinct("file_only").alias("file_count")
)

multi_city_list = (
    city_file_counts.filter("file_count > 1")
    .select("city")
    .rdd.flatMap(lambda x: x)
    .collect()
)
single_city_list = (
    city_file_counts.filter("file_count = 1")
    .select("city")
    .rdd.flatMap(lambda x: x)
    .collect()
)


# ------------------- Utility Function -------------------
def create_daily_agg(df_input):
    df = df_input.withColumn("datetime_ts", to_timestamp("date")).withColumn(
        "date", to_date(col("datetime_ts"))
    )

    key_metrics = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "snow_depth",
        "pressure_msl",
        "surface_pressure",
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        "wind_speed_10m",
        "wind_speed_100m",
        "wind_direction_10m",
        "wind_direction_100m",
        "wind_gusts_10m",
    ]

    agg_exprs = [avg(col(m)).alias(m) for m in key_metrics]
    df_daily = df.groupBy("date", "city").agg(*agg_exprs)
    return df_daily.select("date", "city", *key_metrics).orderBy("date", "city")


# ------------------- Multi-file Merging -------------------
df_multi = df_filled.filter(col("city").isin(multi_city_list))

numeric_cols = [
    c
    for c, t in df_multi.dtypes
    if t in ["double", "int", "float", "long"] and c not in ["date", "city"]
]
string_cols = [
    c for c, t in df_multi.dtypes if t in ["string"] and c not in ["date", "city"]
]

merge_exprs = [F.avg(col(c)).alias(c) for c in numeric_cols] + [
    F.first(col(c), ignorenulls=True).alias(c) for c in string_cols
]

df_merged = df_multi.groupBy("date", "city").agg(*merge_exprs)
df_daily_multi = create_daily_agg(df_merged)

# ------------------- Single-file Cities -------------------
df_single = df_filled.filter(col("city").isin(single_city_list))
df_daily_single = create_daily_agg(df_single)

# ------------------- Final Union -------------------
df_daily_final = df_daily_multi.unionByName(df_daily_single).orderBy("date", "city")
# ------------------- Mapping Data -------------------
df_mapping = (
    spark.read.option("header", True)
    .csv(mapping_path)
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("city", regexp_replace("city", "%20", " "))
    .withColumn("city", initcap("city"))
    .withColumn("city", regexp_replace("city", " ", ""))
    .withColumn("state", initcap("state"))
)

df_mapped = df_daily_final.join(
    df_mapping.select("city", "state", "latitude", "longitude"), on="city", how="left"
)

df_null_mapped = df_mapped.filter(
    col("state").isNull() | col("latitude").isNull() | col("longitude").isNull()
)

# ------------------- Write Output -------------------
df_mapped.write.mode("overwrite").partitionBy("state").parquet(output_path)

# ------------------- Commit Job -------------------
job.commit()
