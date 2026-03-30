# --- Latest Amazon Linux 2023 AMI ---

data "aws_ami" "al2023" {
  count       = var.ami_id == "" ? 1 : 0
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  ami_id = var.ami_id != "" ? var.ami_id : data.aws_ami.al2023[0].id
}

# --- EC2 Security Group ---

resource "aws_security_group" "ec2" {
  name_prefix = "${var.project_name}-ec2-"
  vpc_id      = aws_vpc.main.id
  description = "Omnigraph EC2 instance"

  tags = { Name = "${var.project_name}-ec2" }

  lifecycle { create_before_destroy = true }
}

resource "aws_vpc_security_group_ingress_rule" "ec2_from_alb" {
  security_group_id            = aws_security_group.ec2.id
  description                  = "From ALB on 8080"
  from_port                    = 8080
  to_port                      = 8080
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.alb.id
}

resource "aws_vpc_security_group_egress_rule" "ec2_outbound" {
  security_group_id = aws_security_group.ec2.id
  description       = "Outbound internet (NAT)"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# --- IAM Role for EC2 (SSM + SSM Parameter access) ---

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ec2" {
  name               = "${var.project_name}-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "read_bearer_token" {
  statement {
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.bearer_token.arn]
  }
}

resource "aws_iam_role_policy" "read_bearer_token" {
  name   = "read-bearer-token"
  role   = aws_iam_role.ec2.id
  policy = data.aws_iam_policy_document.read_bearer_token.json
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project_name}-ec2"
  role = aws_iam_role.ec2.name
}

# --- EC2 Instance ---

resource "aws_instance" "omnigraph" {
  ami                    = local.ami_id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name
  key_name               = var.key_pair_name != "" ? var.key_pair_name : null

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = base64encode(templatefile("${path.module}/templates/user_data.sh", {
    aws_region     = var.aws_region
    ssm_token_name = aws_ssm_parameter.bearer_token.name
    ebs_device     = "/dev/xvdf"
    data_dir       = "/var/lib/omnigraph"
    repo_name      = var.repo_name
    config_dir     = "/etc/omnigraph"
    bin_dir        = "/opt/omnigraph/bin"
  }))

  tags = { Name = var.project_name }

  depends_on = [aws_nat_gateway.main]
}

# --- Dedicated EBS Data Volume ---

resource "aws_ebs_volume" "data" {
  availability_zone = aws_subnet.private[0].availability_zone
  size              = var.ebs_volume_size_gb
  type              = "gp3"

  tags = { Name = "${var.project_name}-data" }
}

resource "aws_volume_attachment" "data" {
  device_name = "/dev/xvdf"
  volume_id   = aws_ebs_volume.data.id
  instance_id = aws_instance.omnigraph.id
}
