# =============================================================================
# GitHub OIDC Provider
# =============================================================================

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]

  tags = { Name = "github-actions" }
}

# =============================================================================
# GitHub Actions IAM Role
# =============================================================================

data "aws_iam_policy_document" "github_actions_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = var.github_oidc_subjects
    }
  }
}

resource "aws_iam_role" "github_actions" {
  name               = "${var.project_name}-github-actions"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume.json
  tags               = { Name = "${var.project_name}-github-actions" }
}

data "aws_iam_policy_document" "github_actions_ci" {
  # CodeBuild
  statement {
    actions = [
      "codebuild:StartBuild",
      "codebuild:BatchGetBuilds",
    ]
    resources = [aws_codebuild_project.package.arn]
  }

  # Artifact bucket
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

  # ECR (read-only for inspecting images)
  statement {
    actions = [
      "ecr:DescribeRepositories",
      "ecr:DescribeImages",
    ]
    resources = [aws_ecr_repository.omnigraph_server.arn]
  }

  # Preview deploy parameter reads/writes.
  statement {
    actions = [
      "ssm:GetParameter",
      "ssm:PutParameter",
    ]
    resources = [
      aws_ssm_parameter.bearer_token.arn,
      aws_ssm_parameter.repo_target_uri.arn,
      aws_ssm_parameter.server_image.arn,
    ]
  }

  # Team bearer token materialization for preview deploys.
  statement {
    actions = [
      "ssm:GetParametersByPath",
    ]
    resources = [
      "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/server/tokens/*",
    ]
  }

  # Run Command orchestration for the preview EC2 host.
  statement {
    actions = [
      "ssm:SendCommand",
      "ssm:GetCommandInvocation",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "github_actions_ci" {
  name   = "github-actions-ci"
  role   = aws_iam_role.github_actions.id
  policy = data.aws_iam_policy_document.github_actions_ci.json
}
