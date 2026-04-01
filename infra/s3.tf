# --- Data source for current account/region ---

data "aws_caller_identity" "current" {}

locals {
  account_id           = data.aws_caller_identity.current.account_id
  repo_bucket_name     = var.repo_bucket_name != "" ? var.repo_bucket_name : "${var.project_name}-repo-${local.account_id}-${var.aws_region}"
  artifact_bucket_name = var.artifact_bucket_name != "" ? var.artifact_bucket_name : "${var.project_name}-artifacts-${local.account_id}-${var.aws_region}"
  logs_bucket_name     = var.logs_bucket_name != "" ? var.logs_bucket_name : "${var.project_name}-s3-logs-${local.account_id}-${var.aws_region}"
}

# =============================================================================
# S3 Access Logs Bucket (must exist before other buckets reference it)
# =============================================================================

resource "aws_s3_bucket" "s3_logs" {
  bucket = local.logs_bucket_name
  tags   = { Name = local.logs_bucket_name }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "s3_logs" {
  bucket = aws_s3_bucket.s3_logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "s3_logs" {
  bucket                  = aws_s3_bucket.s3_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "s3_logs" {
  bucket = aws_s3_bucket.s3_logs.id

  rule {
    id     = "expire-logs"
    status = "Enabled"
    filter {}
    expiration { days = 90 }
  }
}

resource "aws_s3_bucket_policy" "s3_logs_tls_only" {
  bucket = aws_s3_bucket.s3_logs.id
  policy = data.aws_iam_policy_document.s3_logs_policy.json
}

data "aws_iam_policy_document" "s3_logs_policy" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = ["${aws_s3_bucket.s3_logs.arn}", "${aws_s3_bucket.s3_logs.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # Allow S3 logging service to write access logs
  statement {
    sid       = "AllowS3Logging"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.s3_logs.arn}/*"]
    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        aws_s3_bucket.omnigraph_repo.arn,
        aws_s3_bucket.omnigraph_artifacts.arn,
      ]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

# =============================================================================
# Repo Bucket
# =============================================================================

resource "aws_s3_bucket" "omnigraph_repo" {
  bucket = local.repo_bucket_name
  tags   = { Name = local.repo_bucket_name }
}

resource "aws_s3_bucket_versioning" "omnigraph_repo" {
  bucket = aws_s3_bucket.omnigraph_repo.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "omnigraph_repo" {
  bucket = aws_s3_bucket.omnigraph_repo.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "omnigraph_repo" {
  bucket                  = aws_s3_bucket.omnigraph_repo.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "omnigraph_repo" {
  bucket = aws_s3_bucket.omnigraph_repo.id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_logging" "omnigraph_repo" {
  bucket        = aws_s3_bucket.omnigraph_repo.id
  target_bucket = aws_s3_bucket.s3_logs.id
  target_prefix = "repo/"
}

resource "aws_s3_bucket_policy" "omnigraph_repo_tls_only" {
  bucket = aws_s3_bucket.omnigraph_repo.id
  policy = data.aws_iam_policy_document.repo_tls_only.json
}

data "aws_iam_policy_document" "repo_tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = ["${aws_s3_bucket.omnigraph_repo.arn}", "${aws_s3_bucket.omnigraph_repo.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

# =============================================================================
# Artifact Bucket
# =============================================================================

resource "aws_s3_bucket" "omnigraph_artifacts" {
  bucket = local.artifact_bucket_name
  tags   = { Name = local.artifact_bucket_name }
}

resource "aws_s3_bucket_versioning" "omnigraph_artifacts" {
  bucket = aws_s3_bucket.omnigraph_artifacts.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "omnigraph_artifacts" {
  bucket = aws_s3_bucket.omnigraph_artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "omnigraph_artifacts" {
  bucket                  = aws_s3_bucket.omnigraph_artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "omnigraph_artifacts" {
  bucket = aws_s3_bucket.omnigraph_artifacts.id

  rule {
    id     = "expire-scratch"
    status = "Enabled"
    filter { prefix = "scratch/" }
    expiration { days = 7 }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_logging" "omnigraph_artifacts" {
  bucket        = aws_s3_bucket.omnigraph_artifacts.id
  target_bucket = aws_s3_bucket.s3_logs.id
  target_prefix = "artifacts/"
}

resource "aws_s3_bucket_policy" "omnigraph_artifacts_tls_only" {
  bucket = aws_s3_bucket.omnigraph_artifacts.id
  policy = data.aws_iam_policy_document.artifacts_tls_only.json
}

data "aws_iam_policy_document" "artifacts_tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = ["${aws_s3_bucket.omnigraph_artifacts.arn}", "${aws_s3_bucket.omnigraph_artifacts.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

# =============================================================================
# S3 Gateway VPC Endpoint
# =============================================================================

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"

  route_table_ids = [aws_route_table.private.id]

  tags = { Name = "${var.project_name}-s3" }
}
