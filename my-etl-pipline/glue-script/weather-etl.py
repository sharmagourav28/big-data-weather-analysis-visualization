import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    regexp_replace,
    initcap,
    to_timestamp,
    to_date,
    hour,
    when,
    lit,
    concat_ws,
    avg,
    first,
)

# ======================== GLUE CONTEXT SETUP ========================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ======================== CONFIGURATION ========================
input_path = "s3://fullautomatedbucket/files/"
output_path = "s3://fullautomatedbucket/cleaned_data/"

# ======================== READ DATA ========================
df_raw = (
    spark.read.option("header", True)
    .csv(input_path + "*.csv")
    .withColumn("file_only", input_file_name())
)

# ======================== CITY NAME EXTRACTION ========================
df_with_city = (
    df_raw.withColumn(
        "city", regexp_extract(col("file_only"), r"/([^/]+?)(?:_\d+)?\.csv$", 1)
    )
    .withColumn("city", regexp_replace("city", "%20", " "))
    .withColumn("city", initcap("city"))
    .withColumn("city", regexp_replace("city", " ", ""))
)


#
# ======================== HANDLE NULLS ========================
df_filled = df_with_city.fillna({"snow_depth": 0})

# ======================== CITY CLASSIFICATION ========================
city_counts = df_filled.groupBy("city").agg(
    F.countDistinct("file_only").alias("file_count")
)
multi_city_list = (
    city_counts.filter("file_count > 1")
    .select("city")
    .rdd.flatMap(lambda x: x)
    .collect()
)
single_city_list = (
    city_counts.filter("file_count = 1")
    .select("city")
    .rdd.flatMap(lambda x: x)
    .collect()
)


# ======================== 12-HOUR AGGREGATION FUNCTION ========================
def create_12hr_agg(df_input):
    df = (
        df_input.withColumn("datetime_ts", to_timestamp("date"))
        .withColumn("hour", hour("datetime_ts"))
        .withColumn("day_part", when(col("hour") < 12, "AM").otherwise("PM"))
        .withColumn("date_only", to_date("datetime_ts"))
        .withColumn(
            "new_datetime",
            concat_ws(
                " ",
                col("date_only"),
                when(col("day_part") == "AM", lit("00:00:00")).otherwise(
                    lit("12:00:00")
                ),
            ),
        )
    )

    metrics = [
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

    agg_exprs = [avg(col(m)).alias(m) for m in metrics]
    df_agg = df.groupBy("new_datetime", "city").agg(*agg_exprs)

    return df_agg.select("new_datetime", "city", *metrics).orderBy(
        "new_datetime", "city"
    )


# ======================== MULTI-FILE CITIES ========================
df_multi = df_filled.filter(col("city").isin(multi_city_list))

# Determine columns by data type
numeric_cols = [
    c
    for c, t in df_multi.dtypes
    if t in ["double", "int", "float", "long"] and c not in ["date", "city"]
]
string_cols = [
    c for c, t in df_multi.dtypes if t == "string" and c not in ["date", "city"]
]

merge_exprs = [avg(col(c)).alias(c) for c in numeric_cols] + [
    first(col(c), ignorenulls=True).alias(c) for c in string_cols
]

df_merged = df_multi.groupBy("date", "city").agg(*merge_exprs)
df_12hr_multi = create_12hr_agg(df_merged)

# ======================== SINGLE-FILE CITIES ========================
df_single = df_filled.filter(col("city").isin(single_city_list))
df_12hr_single = create_12hr_agg(df_single)

# ======================== FINAL UNION ========================
df_12hr_final = df_12hr_multi.unionByName(df_12hr_single).orderBy(
    "new_datetime", "city"
)


# ======================== WRITE TO S3 ========================
final_cols = [
    "new_datetime",
    "city",
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

df_12hr_final.select(final_cols).coalesce(1).write.mode("overwrite").option(
    "header", "true"
).csv(output_path)


# ======================== END GLUE JOB ========================
job.commit()
