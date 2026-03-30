output "cloudfront_domain" {
  description = "CloudFront distribution domain"
  value       = aws_cloudfront_distribution.main.domain_name
}

output "alb_dns_name" {
  description = "ALB DNS name (origin, not for direct use)"
  value       = aws_lb.main.dns_name
}

output "endpoint_url" {
  description = "Primary endpoint URL"
  value       = var.domain_name != "" ? "https://${var.hostname}.${var.domain_name}" : "https://${aws_cloudfront_distribution.main.domain_name}"
}

output "instance_id" {
  description = "EC2 instance ID (use with SSM Session Manager)"
  value       = aws_instance.omnigraph.id
}

output "ssm_connect_command" {
  description = "Command to connect via SSM"
  value       = "aws ssm start-session --target ${aws_instance.omnigraph.id} --region ${var.aws_region}"
}
