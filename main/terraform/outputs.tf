output "bucket_name" {
  value = aws_s3_bucket.my_bucket.bucket
  description = "The name of the created S3 bucket"
}

output "instance_public_ip" {
  value = aws_instance.pandas-ec2.instance_public_ip
  description = "The public IP address of the EC2 instance"
}
