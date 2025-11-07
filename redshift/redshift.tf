provider "aws" {
  region = "eu-north-1"
}

# 1. VPC and Subnet Group (assumes existing VPC)
data "aws_vpc" "default" {
  default = true
}

data "aws_subnet_ids" "default" {
  vpc_id = data.aws_vpc.default.id
}

resource "aws_redshift_subnet_group" "redshift_subnet" {
  name       = "redshift-subnet-group"
  subnet_ids = data.aws_subnet_ids.default.ids
  tags = {
    Name = "Redshift Subnet Group"
  }
}

# 2. Security Group
resource "aws_security_group" "redshift_sg" {
  name        = "redshift-sg"
  description = "Allow Redshift access"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Restrict in production
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "Redshift SG"
  }
}

# 3. IAM Role for Redshift
resource "aws_iam_role" "redshift_role" {
  name = "redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "redshift.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "redshift_policy" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# 4. Redshift Cluster
resource "aws_redshift_cluster" "redshift" {
  cluster_identifier = "finance-data-cluster"
  node_type          = "dc2.large"
  master_username    = "adminuser"
  master_password    = "YourSecurePassword123!"
  cluster_type       = "single-node"
  iam_roles          = [aws_iam_role.redshift_role.arn]
  db_name            = "finance"
  port               = 5439

  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet.name

  tags = {
    Environment = "dev"
    Project     = "finance-data-pipeline"
  }
}
