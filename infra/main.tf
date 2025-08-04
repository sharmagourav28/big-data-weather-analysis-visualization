resource "aws_s3_bucket" "etl_bucket" {
  bucket = var.bucket_name
}

resource "aws_glue_catalog_database" "etl_db" {
  name = "weather_db"
}

# REMOVE or COMMENT THIS BLOCK:
# resource "aws_iam_role" "glue_role" { ... }

# REMOVE or COMMENT THIS BLOCK:
# resource "aws_iam_role_policy_attachment" "glue_role_policy" { ... }

# Replace with your existing role ARN
locals {
  glue_role_arn = "arn:aws:iam::236884234329:role/labrole"
}

resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = "3"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
}

resource "aws_glue_crawler" "etl_crawler" {
  name          = var.glue_crawler_name
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "s3://${var.bucket_name}/cleaned_data/"
  }

  depends_on = [aws_glue_job.etl_job]
}
