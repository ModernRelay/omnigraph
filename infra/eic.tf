# --- EC2 Instance Connect Endpoint ---

resource "aws_security_group" "eic" {
  name_prefix = "${var.project_name}-eic-"
  vpc_id      = aws_vpc.main.id
  description = "EC2 Instance Connect Endpoint"

  tags = { Name = "${var.project_name}-eic" }

  lifecycle { create_before_destroy = true }
}

resource "aws_vpc_security_group_egress_rule" "eic_to_ec2" {
  security_group_id            = aws_security_group.eic.id
  description                  = "SSH to Omnigraph EC2"
  from_port                    = 22
  to_port                      = 22
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.ec2.id
}

resource "aws_vpc_security_group_ingress_rule" "ec2_from_eic" {
  security_group_id            = aws_security_group.ec2.id
  description                  = "SSH from EC2 Instance Connect Endpoint"
  from_port                    = 22
  to_port                      = 22
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.eic.id
}

resource "aws_ec2_instance_connect_endpoint" "main" {
  subnet_id          = aws_subnet.private[0].id
  security_group_ids = [aws_security_group.eic.id]
  preserve_client_ip = false

  tags = { Name = var.project_name }
}
