variable "bucket_name" {
  type = string
  description = "The name of the S3 bucket"
}

variable "instance_type" {
  type = string
  description = "The type of EC2 instance"
  default = "t3.micro"
}

variable "ami_id" {
  type = string
  description = "The AMI Id for the EC2 instance: i.e. Ubuntu"
  default = "ami-0d1b5a8c13042c939" # Default to using Ubuntu AMI for EC2
}

variable "tags" {
  type = map(string)
  description = "Tags to apply to resources"
  default = {
    Environment = "dev"
    Owner = "Timmeh"
  }
}
