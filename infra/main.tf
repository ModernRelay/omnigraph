terraform {
  cloud {
    organization = "collective-lab"

    workspaces {
      name = "omni-context"
    }
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.5"
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "omnigraph"
      Team      = "modernrelay"
      ManagedBy = "terraform"
    }
  }
}
