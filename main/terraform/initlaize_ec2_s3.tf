provider "aws" {
  region = "us-east-2" # ohio server
}
resource "aws_s3_bucket" "gsi_data_bucket" {
  bucket = "gsi_data_bucket"
  acl = "private"

  tags = {
    Name        = "GSIDataBucket"
    Environment = "Dev"
  }
}

resource "aws_iam_role" "ec2_s3_access_role" {
  name = "ec2-s3-access-role"

  assume_role_policy = json_encode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "s3_access_policy" {
  name = "s3-read-access-policy"
  description = "Allows EC2 instance to read from specific s3 bucket"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.gsi_data_bucket.arn,
          "${aws_s3_bucket.gsi_data_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_s3_policy" {
  role = aws_iam_role.ec2_s3_access_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "my-ec2-instance-profile"
  role = aws_iam_role.ec2_s3_access_role.name
}

resource "aws_instance" "my_ec2_instance" {
  ami           = "ami-0d1b5a8c13042c939" # check bottom for how to find value installed
  instance_type = "t3.micro"
  iam_instance_profile = aws_iam_instance_profile.ec2_instance_profile.name
  #key_name      = "my-ssh-key" # Replace with your SSH key pair name: use  ~$ ssh-keygen -y -f my-key.pem
  #security_groups = ["my-security-group"] # Replace with your security group name or ID

  tags = {
    Name        = "MyTerraformEC2"
    Environment = "Dev"
  }
}

# how to find the ami installed on an EC2,
# 1) manually set up ec2 online, choose ubuntu
# 2) launch ec2 console
# 3) on console, sudo snap install aws-cli --classic
# 4) get ec2 instance id from online dashboard where launced ec2 from
# 5) on colse: bash~$ aws ec2 describe-instances --instance-ids your-instance-id --query "Reservations[].Instances[].ImageId" --output text\



#aws ec2 describe-instances --instance-ids your-instance-id --query "Reservations[].Instances[].ImageId" --output text



#---
# Terraform commands
# Terraform init
# Terraform plan
# Terraform apply
# terraform destroy -auto-approve
