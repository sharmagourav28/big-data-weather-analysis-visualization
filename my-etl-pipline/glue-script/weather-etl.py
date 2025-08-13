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

import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    regexp_extract,
    regexp_replace,
    initcap,
    to_timestamp,
    to_date,
)

# raw data path
base_paths = [
    "s3://allweatherrdatasetstatelatlongwd1/wd1data/",
    "s3://allweatherstatelatlongwd2/wd2statedata/",
    "s3://allweatherdatastatelaglongwd3/files/",
]

# state wise city mapping file path
mapping_path = (
    "s3://citystatelatlongregion/city_state_lat_long_region/cities_with_region.csv"
)
# output file locations
output_path_daily = "s3://fullautomatedbucketterraformone281/weatherdata/"

df_all = (
    spark.read.option("header", True)
    .parquet(*base_paths)
    .drop("pressure_msl", "surface_pressure")
)


df_cleaned = df_all.withColumn(
    "city", regexp_extract(col("file_only"), r"/([^/]+?)(?:_\d+)?\.csv$", 1)
)

df_with_city = (
    df_cleaned.withColumn("city", regexp_replace("city", "%20", " "))
    .withColumn("city", initcap("city"))
    .withColumn("city", regexp_replace("city", " ", ""))
)

df_filled = df_with_city.fillna({"snow_depth": 0})


# ------------------- City Classification -------------------
city_file_counts = df_with_city.groupBy("city").agg(
    F.countDistinct("file_only").alias("file_count")
)

# city which have more than 1 file count
multi_city_list = (
    city_file_counts.filter("file_count > 1")
    .select("city")
    .rdd.flatMap(lambda x: x)
    .collect()
)

# city which have only 1 file count
single_city_list = (
    city_file_counts.filter("file_count = 1")
    .select("city")
    .rdd.flatMap(lambda x: x)
    .collect()
)


# ------------------- Aggregation Function -------------------
def create_daily_agg(df_input):
    df = (
        df_input.withColumn("datetime_ts", to_timestamp("date"))
        .withColumn("date", to_date(col("datetime_ts")))
        .withColumn("year", F.year(col("date")))
        .withColumn("month", F.month(col("date")))
    )

    agg_exprs = [
        F.avg("temperature_2m").alias("temperature_2m"),
        F.avg("relative_humidity_2m").alias("relative_humidity_2m"),
        F.avg("dew_point_2m").alias("dew_point_2m"),
        F.avg("apparent_temperature").alias("apparent_temperature"),
        F.sum("precipitation").alias("precipitation"),
        F.sum("rain").alias("rain"),
        F.sum("snowfall").alias("snowfall"),
        F.sum("snow_depth").alias("snow_depth"),
        F.avg("cloud_cover").alias("cloud_cover"),
        F.avg("cloud_cover_low").alias("cloud_cover_low"),
        F.avg("cloud_cover_mid").alias("cloud_cover_mid"),
        F.avg("cloud_cover_high").alias("cloud_cover_high"),
        F.avg("wind_speed_10m").alias("wind_speed_10m"),
        F.avg("wind_speed_100m").alias("wind_speed_100m"),
        F.avg("wind_direction_10m").alias("wind_direction_10m"),
        F.avg("wind_direction_100m").alias("wind_direction_100m"),
        F.max("wind_gusts_10m").alias("wind_gusts_10m"),
    ]
    df_daily = df.groupBy("date", "year", "month", "city").agg(*agg_exprs)
    return df_daily.orderBy("date", "city")


# ==================== MULTI-FILE MERGING ====================
df_multi = df_filled.filter(col("city").isin(multi_city_list))


# Merge multiple files by date + city
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
df_12hr_multi = create_daily_agg(df_merged)


# ==================== SINGLE FILE CITIES ====================
df_single = df_filled.filter(col("city").isin(single_city_list))
df_12hr_single = create_daily_agg(df_single)


# ==================== FINAL UNION ====================
df_12hr_final = df_12hr_multi.unionByName(df_12hr_single)


# ------------------- Mapping Data with Region -------------------
df_mapping = (
    spark.read.option("header", True)
    .csv(mapping_path)
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("city", regexp_replace("city", "%20", " "))
    .withColumn("city", initcap("city"))
    .withColumn("city", regexp_replace("city", " ", ""))
    .withColumn("state", initcap("state"))
    .withColumn("region", initcap("region"))
)

df_mapped = df_12hr_final.join(
    df_mapping.select("city", "state", "latitude", "longitude", "region"),
    on="city",
    how="left",
)


df_final = df_mapped.withColumn("month", F.month("date")).withColumn(
    "season",
    F.when(F.col("month").isin(12, 1, 2), "Winter")
    .when(F.col("month").isin(3, 4, 5), "Summer")
    .when(F.col("month").isin(6, 7, 8, 9), "Monsoon")
    .otherwise("Post-Monsoon"),
)


df_final.repartition("state").write.mode("overwrite").partitionBy("state").parquet(
    output_path_daily
)

# ------------------- Commit Job -------------------
job.commit()
