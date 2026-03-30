# --- ALB Security Group ---

resource "aws_security_group" "alb" {
  name_prefix = "${var.project_name}-alb-"
  vpc_id      = aws_vpc.main.id
  description = "ALB ingress"

  tags = { Name = "${var.project_name}-alb" }

  lifecycle { create_before_destroy = true }
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTP from CloudFront / internet"
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
  cidr_ipv4         = var.allowed_ingress_cidrs[0]
}

resource "aws_vpc_security_group_egress_rule" "alb_to_ec2" {
  security_group_id            = aws_security_group.alb.id
  description                  = "To Omnigraph EC2 on 8080"
  from_port                    = 8080
  to_port                      = 8080
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.ec2.id
}

# --- Application Load Balancer ---

resource "aws_lb" "main" {
  name               = var.project_name
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id

  tags = { Name = var.project_name }
}

# --- Target Group ---

resource "aws_lb_target_group" "omnigraph" {
  name     = var.project_name
  port     = 8080
  protocol = "HTTP"
  vpc_id   = aws_vpc.main.id

  health_check {
    path                = "/healthz"
    port                = "traffic-port"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = { Name = var.project_name }
}

resource "aws_lb_target_group_attachment" "omnigraph" {
  target_group_arn = aws_lb_target_group.omnigraph.arn
  target_id        = aws_instance.omnigraph.id
  port             = 8080
}

# --- HTTP Listener ---
# CloudFront terminates TLS; ALB serves HTTP only.

# Open forward (no origin secret)
resource "aws_lb_listener" "http" {
  count             = var.cloudfront_origin_secret == "" ? 1 : 0
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.omnigraph.arn
  }
}

# Locked down (with origin secret): deny by default, allow via header match
resource "aws_lb_listener" "http_locked" {
  count             = var.cloudfront_origin_secret != "" ? 1 : 0
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "fixed-response"

    fixed_response {
      content_type = "text/plain"
      message_body = "Forbidden"
      status_code  = "403"
    }
  }
}

resource "aws_lb_listener_rule" "allow_cloudfront" {
  count        = var.cloudfront_origin_secret != "" ? 1 : 0
  listener_arn = aws_lb_listener.http_locked[0].arn
  priority     = 1

  condition {
    http_header {
      http_header_name = "X-Origin-Verify"
      values           = [var.cloudfront_origin_secret]
    }
  }

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.omnigraph.arn
  }
}
