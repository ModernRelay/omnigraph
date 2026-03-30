resource "aws_ssm_parameter" "bearer_token" {
  name        = "/${var.project_name}/server/bearer-token"
  description = "Omnigraph server bearer token"
  type        = "SecureString"
  value       = var.bearer_token
}
