resource "aws_s3_bucket" "etl_bucket" {
  bucket = var.bucket_name
}

resource "aws_glue_catalog_database" "etl_db" {
  name = "weather_db"
}

resource "aws_iam_role" "glue_role" {
  name = "glue-service-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Effect = "Allow"
      Sid    = ""
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_role_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_role.arn
  command {
    name            = "glueetl"
    script_location = var.script_s3_path
    python_version  = "3"
  }
  glue_version      = "4.0"
  max_capacity      = 2
  number_of_workers = 2
  worker_type       = "G.1X"
}

resource "aws_glue_crawler" "etl_crawler" {
  name          = var.glue_crawler_name
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.etl_db.name
  s3_target {
    path = "s3://${var.bucket_name}/cleaned_data/"
  }
  depends_on = [aws_glue_job.etl_job]
}
