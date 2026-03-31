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
