#declare a region
variable "region" {
  default = "us-east-1"
}

#declare a bucket name
variable "bucket_name_prefix" {
  default = "fullautomatedbucketterraformone11"
}


#declare a glue job name
variable "glue_job_name" {
  default = "glue-etl-job"
}

#declare a crawler name
variable "glue_crawler_name" {
  default = "my-etl-crawler"
}

#declare a script path
variable "script_s3_path" {
  default = "s3://fullautomatedbucketterraformone11/scripts/weather-etl.py"
}
