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

# --- S3 ---

output "repo_bucket_name" {
  description = "S3 bucket for Omnigraph repos"
  value       = aws_s3_bucket.omnigraph_repo.id
}

output "artifact_bucket_name" {
  description = "S3 bucket for build artifacts"
  value       = aws_s3_bucket.omnigraph_artifacts.id
}

output "logs_bucket_name" {
  description = "S3 bucket for access logs"
  value       = aws_s3_bucket.s3_logs.id
}

# --- Build ---

output "codebuild_project_name" {
  description = "CodeBuild project name"
  value       = aws_codebuild_project.package.name
}

output "ecr_repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.omnigraph_server.repository_url
}

# --- OIDC ---

output "github_actions_role_arn" {
  description = "GitHub Actions OIDC role ARN"
  value       = aws_iam_role.github_actions.arn
}

# --- Runtime ---

output "repo_target_uri_param" {
  description = "SSM parameter name for active repo target URI"
  value       = aws_ssm_parameter.repo_target_uri.name
}

output "server_image_param" {
  description = "SSM parameter name for the active omnigraph-server image reference"
  value       = aws_ssm_parameter.server_image.name
}
