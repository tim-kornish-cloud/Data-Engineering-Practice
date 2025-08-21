provider "aws" {
	region = "us-east-2" # ohio server
}

# set up S3 bucket
resource "aws_s3_bucket" "my_bucket" {
	bucket = var.bucket_name # bucket name using youtube tutorial
	tags = var.tags
}
# set up an EC2 instance
resource "aws_instance" "pandas-ec2" {
	ami = var.ami_id
	instance_type = var.instance_type
	tags = var.tags
}



# install aws cli
# create use on AWS
# create access key for new (non-root user) user
# on console: aws configure, set values to connect aws-cli with user on aws cloud

# Console commands using windows console to use terraform

# 1) ~$ terraform init
# 2) ~$ terraform plan
# 3) ~$ terraform apply
# 4) ~$ yes
# 5) ~$ terraform destroy -auto-approve
