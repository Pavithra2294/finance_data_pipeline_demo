
resource "aws_cloudwatch_metric_alarm" "ec2_cpu_high" {
  alarm_name          = "HighCPUUtilization"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 80
  alarm_description   = "Alarm when EC2 CPU exceeds 80%"
  dimensions = {
    InstanceId = var.ec2_instance_id
  }
  alarm_actions       = [var.sns_topic_arn]
  ok_actions          = [var.sns_topic_arn]
}

resource "aws_cloudwatch_metric_alarm" "s3_bucket_size" {
  alarm_name          = "S3BucketSizeAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "BucketSizeBytes"
  namespace           = "AWS/S3"
  period              = 86400
  statistic           = "Maximum"
  threshold           = 1000000000 # 1 GB
  alarm_description   = "Alarm when S3 bucket size exceeds 1GB"
  dimensions = {
    BucketName = var.s3_bucket_name
    StorageType = "StandardStorage"
  }
  alarm_actions       = [var.sns_topic_arn]
}

variable "ec2_instance_id" {
  description = "EC2 instance to monitor"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket to monitor"
  type        = string
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for alarm notifications"
  type        = string
}
