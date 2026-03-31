#!/bin/bash
set -euo pipefail

# --- Wait for EBS volume to attach ---
while [ ! -b ${ebs_device} ]; do
  echo "Waiting for EBS volume ${ebs_device}..."
  sleep 2
done

# --- Format if unformatted, then mount ---
if ! blkid ${ebs_device}; then
  mkfs.xfs ${ebs_device}
fi

mkdir -p ${data_dir}
mount ${ebs_device} ${data_dir}

# Persist mount across reboots
if ! grep -q "${ebs_device}" /etc/fstab; then
  echo "${ebs_device} ${data_dir} xfs defaults,nofail 0 2" >> /etc/fstab
fi

mkdir -p ${data_dir}/data

# --- Ensure SSM agent is installed and running ---
dnf install -y amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# --- Ensure EC2 Instance Connect is installed ---
dnf install -y ec2-instance-connect

# --- Create omnigraph system user ---
useradd --system --no-create-home --shell /sbin/nologin omnigraph || true
chown -R omnigraph:omnigraph ${data_dir}

# --- Install directories ---
mkdir -p ${bin_dir}
mkdir -p ${config_dir}

# --- Fetch bearer token from SSM ---
TOKEN=$(aws ssm get-parameter \
  --name "${ssm_token_name}" \
  --with-decryption \
  --region ${aws_region} \
  --query 'Parameter.Value' \
  --output text)

GEMINI_KEY=$(aws ssm get-parameter \
  --name "${ssm_gemini_key_name}" \
  --with-decryption \
  --region ${aws_region} \
  --query 'Parameter.Value' \
  --output text)

cat > ${config_dir}/server.env <<ENVEOF
OMNIGRAPH_SERVER_BEARER_TOKEN=$TOKEN
GEMINI_API_KEY=$GEMINI_KEY
ENVEOF

chmod 600 ${config_dir}/server.env
chown omnigraph:omnigraph ${config_dir}/server.env

# --- Write omnigraph.yaml ---
cat > ${config_dir}/omnigraph.yaml <<YAMLEOF
targets:
  local:
    uri: ${data_dir}/data/${repo_name}

server:
  target: local
  bind: 0.0.0.0:8080

policy: {}
YAMLEOF

chown omnigraph:omnigraph ${config_dir}/omnigraph.yaml

# --- Install systemd unit ---
cat > /etc/systemd/system/omnigraph-server.service <<UNITEOF
[Unit]
Description=Omnigraph Server
After=network-online.target local-fs.target
Wants=network-online.target
ConditionPathExists=${data_dir}/data/${repo_name}

[Service]
Type=simple
User=omnigraph
Group=omnigraph
EnvironmentFile=${config_dir}/server.env
ExecStart=${bin_dir}/omnigraph-server --config ${config_dir}/omnigraph.yaml
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNITEOF

systemctl daemon-reload
systemctl enable omnigraph-server.service

# --- Note: the service won't start until the binary is deployed
#     and the repo is bootstrapped at ${data_dir}/data/${repo_name}. ---
echo "Omnigraph infra bootstrap complete. Deploy binary and bootstrap repo to start the service."
