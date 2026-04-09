variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "omnigraph"
}

# --- Networking ---

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  type    = list(string)
  default = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "private_subnet_cidrs" {
  type    = list(string)
  default = ["10.0.10.0/24", "10.0.11.0/24"]
}

variable "allowed_ingress_cidrs" {
  description = "CIDRs allowed to reach the ALB on 80. Restrict to CloudFront-only via origin header in production."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# --- Compute ---

variable "instance_type" {
  description = "EC2 instance type. t3.large (2 vCPU/8 GiB) or t3.xlarge (4 vCPU/16 GiB) for heavier workloads."
  type        = string
  default     = "t3.large"
}

variable "ami_id" {
  description = "AMI ID for the EC2 instance. Leave empty to use latest Amazon Linux 2023."
  type        = string
  default     = ""
}

variable "key_pair_name" {
  description = "Optional EC2 key pair for SSH (SSM is preferred)."
  type        = string
  default     = ""
}

# --- Storage ---

variable "ebs_volume_size_gb" {
  description = "Size of the dedicated Omnigraph data volume in GiB."
  type        = number
  default     = 50
}

variable "repo_name" {
  description = "Name of the Omnigraph repo file on disk."
  type        = string
  default     = "context.omni"
}

# --- Auth ---

variable "bearer_token" {
  description = "Bearer token for the Omnigraph server API. Stored in SSM Parameter Store."
  type        = string
  sensitive   = true
}

variable "gemini_api_key" {
  description = "Gemini API key. Stored in SSM Parameter Store."
  type        = string
  sensitive   = true
}

# --- S3 Storage ---

variable "repo_bucket_name" {
  description = "Override repo bucket name. Defaults to omnigraph-repo-<account>-<region>."
  type        = string
  default     = ""
}

variable "artifact_bucket_name" {
  description = "Override artifact bucket name. Defaults to omnigraph-artifacts-<account>-<region>."
  type        = string
  default     = ""
}

variable "logs_bucket_name" {
  description = "Override logs bucket name. Defaults to omnigraph-s3-logs-<account>-<region>."
  type        = string
  default     = ""
}

variable "repo_graph_name" {
  description = "Name of the graph inside the repo bucket."
  type        = string
  default     = "context"
}

variable "initial_repo_target_uri" {
  description = "Initial S3 URI for the live graph repo. Leave empty for local-disk fallback."
  type        = string
  default     = ""
}

variable "initial_server_image" {
  description = "Initial container image reference for omnigraph-server. Leave empty to use the bootstrap tag placeholder."
  type        = string
  default     = ""
}

# --- Build ---

variable "codebuild_compute_type" {
  description = "CodeBuild compute type."
  type        = string
  default     = "BUILD_GENERAL1_MEDIUM"
}

variable "codebuild_image" {
  description = "CodeBuild image. Must be AL2023-compatible x86_64."
  type        = string
  default     = "aws/codebuild/amazonlinux-x86_64-standard:5.0"
}

variable "github_repository" {
  description = "GitHub repository in owner/repo format."
  type        = string
  default     = "ModernRelay/omnigraph"
}

variable "github_oidc_subjects" {
  description = "Allowed OIDC sub claims for GitHub Actions."
  type        = list(string)
  default = [
    "repo:ModernRelay/omnigraph:ref:refs/heads/*",
    "repo:ModernRelay/omnigraph:ref:refs/tags/*",
    "repo:ModernRelay/omnigraph-platform:ref:refs/heads/*",
    "repo:ModernRelay/omnigraph-platform:ref:refs/tags/*",
  ]
}

# --- DNS / CloudFront ---

variable "domain_name" {
  description = "Root domain, e.g. example.com. Leave empty to skip DNS."
  type        = string
  default     = ""
}

variable "hostname" {
  description = "Subdomain prefix, e.g. omnigraph."
  type        = string
  default     = "omnigraph"
}

variable "acm_certificate_arn" {
  description = "ARN of an ACM certificate in us-east-1 for CloudFront. Required for HTTPS."
  type        = string
  default     = ""
}

variable "cloudfront_origin_secret" {
  description = "Shared secret header between CloudFront and ALB to restrict direct ALB access."
  type        = string
  sensitive   = true
  default     = ""
}
