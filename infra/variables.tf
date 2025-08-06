variable "region" {
  default = "us-east-1"
}

variable "bucket_name_prefix" {
  default = "fullautomatedbucketterraformone"
}

variable "glue_job_name" {
  default = "glue-etl-job"
}

variable "glue_crawler_name" {
  default = "my-etl-crawler"
}

variable "script_s3_path" {
  default = "s3://fullautomatedbucketterraformone/scripts/weather-etl.py"
}
