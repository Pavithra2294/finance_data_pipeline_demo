
# CloudWatch Log Group for general application logs
resource "aws_cloudwatch_log_group" "app_logs" {
  name              = "/financial-pipeline/app"
  retention_in_days = 14

  tags = {
    Environment = "dev"
    Project     = "financial-data-pipeline"
  }
}

# CloudWatch Log Group for Airflow logs (if using EC2 or ECS)
resource "aws_cloudwatch_log_group" "airflow_logs" {
  name              = "/financial-pipeline/airflow"
  retention_in_days = 30

  tags = {
    Environment = "dev"
    Component   = "airflow"
  }
}

# S3 bucket for access logs
resource "aws_s3_bucket" "access_logs" {
  bucket = "my-financial-access-logs"

  tags = {
    Name        = "Access Logs Bucket"
    Environment = "dev"
  }
}

# Enable logging on your main S3 bucket
resource "aws_s3_bucket_logging" "financial_data_logs" {
  bucket = aws_s3_bucket.financial_data.id

  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "s3-access-logs/"
}
