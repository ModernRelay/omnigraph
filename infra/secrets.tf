resource "aws_ssm_parameter" "bearer_token" {
  name        = "/${var.project_name}/server/bearer-token"
  description = "Omnigraph server bearer token"
  type        = "SecureString"
  value       = var.bearer_token
}

resource "aws_ssm_parameter" "gemini_api_key" {
  name        = "/${var.project_name}/server/gemini-api-key"
  description = "Gemini API key"
  type        = "SecureString"
  value       = var.gemini_api_key
}

resource "aws_ssm_parameter" "repo_target_uri" {
  name        = "/${var.project_name}/server/target-uri"
  description = "Active Omnigraph repo target URI"
  type        = "String"
  value       = var.initial_repo_target_uri != "" ? var.initial_repo_target_uri : "file:///var/lib/omnigraph/data/${var.repo_name}"
}
