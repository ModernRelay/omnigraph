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
