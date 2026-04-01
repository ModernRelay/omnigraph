# =============================================================================
# CodeBuild Packaging (AL2023)
# =============================================================================

resource "aws_cloudwatch_log_group" "codebuild_package" {
  name              = "/codebuild/${var.project_name}-package"
  retention_in_days = 30
  tags              = { Name = "${var.project_name}-package" }
}

# --- CodeBuild IAM Role ---

data "aws_iam_policy_document" "codebuild_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["codebuild.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "codebuild_package" {
  name               = "${var.project_name}-codebuild-package"
  assume_role_policy = data.aws_iam_policy_document.codebuild_assume.json
}

data "aws_iam_policy_document" "codebuild_package" {
  # CloudWatch Logs
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.codebuild_package.arn}:*"]
  }

  # Artifact bucket write
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.omnigraph_artifacts.arn,
      "${aws_s3_bucket.omnigraph_artifacts.arn}/*",
    ]
  }

  # ECR push (conditional on repo existing)
  statement {
    actions = [
      "ecr:GetAuthorizationToken",
    ]
    resources = ["*"]
  }

  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:PutImage",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
    ]
    resources = [aws_ecr_repository.omnigraph_server.arn]
  }
}

resource "aws_iam_role_policy" "codebuild_package" {
  name   = "codebuild-package"
  role   = aws_iam_role.codebuild_package.id
  policy = data.aws_iam_policy_document.codebuild_package.json
}

# --- CodeBuild Project ---

resource "aws_codebuild_project" "package" {
  name         = "${var.project_name}-package-al2023"
  description  = "Build omnigraph-server and omnigraph CLI on AL2023"
  service_role = aws_iam_role.codebuild_package.arn

  source {
    type            = "GITHUB"
    location        = "https://github.com/${var.github_repository}.git"
    buildspec       = "buildspec.package.yml"
    git_clone_depth = 1
  }

  environment {
    compute_type    = var.codebuild_compute_type
    image           = var.codebuild_image
    type            = "LINUX_CONTAINER"
    privileged_mode = true

    environment_variable {
      name  = "OMNIGRAPH_ECR_REPOSITORY"
      value = aws_ecr_repository.omnigraph_server.name
    }

    environment_variable {
      name  = "AWS_REGION"
      value = var.aws_region
    }
  }

  artifacts {
    type     = "S3"
    location = aws_s3_bucket.omnigraph_artifacts.id
    path     = "builds"

    packaging              = "NONE"
    encryption_disabled    = false
    override_artifact_name = true
  }

  logs_config {
    cloudwatch_logs {
      group_name = aws_cloudwatch_log_group.codebuild_package.name
    }
  }

  tags = { Name = "${var.project_name}-package" }
}

# =============================================================================
# ECR Repository
# =============================================================================

resource "aws_ecr_repository" "omnigraph_server" {
  name                 = "omnigraph-server"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = { Name = "omnigraph-server" }
}

resource "aws_ecr_lifecycle_policy" "omnigraph_server" {
  repository = aws_ecr_repository.omnigraph_server.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 20 untagged images"
        selection = {
          tagStatus   = "untagged"
          countType   = "imageCountMoreThan"
          countNumber = 20
        }
        action = { type = "expire" }
      },
    ]
  })
}
