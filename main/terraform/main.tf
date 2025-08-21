provider "aws" {
	region = "us-east-2" # ohio server
}

resource "aws_s3_bucket" "my_bucket" {
	bucket = "jama-test-bucket" # bucket name using youtube tutorial
}

#install aws cli
# create use on AWS
# create access key for new (non-root user) user
# on console: aws configure, set values to connect aws-cli with user on aws cloud

# Console commands using windows console

# 1) ~$ terraform init
# 2) ~$ terraform plan
# 3) ~$ terraform apply
# 4) ~$ yes
# 5) ~$ terraform destroy
