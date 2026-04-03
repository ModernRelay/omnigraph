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
dnf install -y amazon-ssm-agent ec2-instance-connect docker
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# --- Ensure Docker is installed and running ---
systemctl enable docker
systemctl start docker

# --- Create omnigraph system user ---
useradd --system --no-create-home --shell /sbin/nologin omnigraph || true
chown -R omnigraph:omnigraph ${data_dir}

# --- Install directories ---
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

TARGET_URI=$(aws ssm get-parameter \
  --name "${ssm_target_uri_name}" \
  --region ${aws_region} \
  --query 'Parameter.Value' \
  --output text)

IMAGE_REF=$(aws ssm get-parameter \
  --name "${ssm_image_name}" \
  --region ${aws_region} \
  --query 'Parameter.Value' \
  --output text)

TOKENS_JSON=$(aws ssm get-parameters-by-path \
  --path "${ssm_tokens_path}" \
  --with-decryption \
  --recursive \
  --region ${aws_region} \
  --query 'Parameters[*].[Name,Value]' \
  --output json | python3 - "${ssm_tokens_path}" <<'PY'
import json
import sys

prefix = sys.argv[1].rstrip("/") + "/"
items = json.load(sys.stdin)
tokens = {}
for name, value in items:
    if name.startswith(prefix):
        actor = name[len(prefix):].strip()
        if actor:
            tokens[actor] = value
print(json.dumps(tokens, separators=(",", ":")))
PY
)

printf '%s\n' "$TOKENS_JSON" > ${config_dir}/bearer-tokens.json
chmod 600 ${config_dir}/bearer-tokens.json
chown omnigraph:omnigraph ${config_dir}/bearer-tokens.json

cat > ${config_dir}/server.env <<ENVEOF
OMNIGRAPH_SERVER_BEARER_TOKEN=$TOKEN
OMNIGRAPH_SERVER_BEARER_TOKENS_FILE=${config_dir}/bearer-tokens.json
GEMINI_API_KEY=$GEMINI_KEY
OMNIGRAPH_TARGET_URI=$TARGET_URI
OMNIGRAPH_SERVER_IMAGE=$IMAGE_REF
AWS_REGION=${aws_region}
ENVEOF

chmod 600 ${config_dir}/server.env
chown omnigraph:omnigraph ${config_dir}/server.env

# --- Write omnigraph.yaml ---
# If OMNIGRAPH_TARGET_URI is set, use it; otherwise fall back to local disk
cat > ${config_dir}/omnigraph.yaml <<YAMLEOF
targets:
  active:
    uri: $TARGET_URI
  local:
    uri: ${data_dir}/data/${repo_name}

server:
  target: active
  bind: 0.0.0.0:8080

policy: {}
YAMLEOF

chown omnigraph:omnigraph ${config_dir}/omnigraph.yaml

# --- Install systemd unit ---
cat > /etc/systemd/system/omnigraph-server.service <<UNITEOF
[Unit]
Description=Omnigraph Server (Docker)
After=network-online.target docker.service local-fs.target
Wants=network-online.target docker.service
Requires=docker.service

[Service]
Type=simple
EnvironmentFile=${config_dir}/server.env
ExecStartPre=-/usr/bin/docker stop omnigraph-server
ExecStartPre=-/usr/bin/docker rm -f omnigraph-server
ExecStartPre=/bin/sh -lc 'test -n "$OMNIGRAPH_SERVER_IMAGE"'
ExecStartPre=/bin/sh -lc 'registry="$${OMNIGRAPH_SERVER_IMAGE%%/*}"; aws ecr get-login-password --region "$${AWS_REGION}" | /usr/bin/docker login --username AWS --password-stdin "$${registry}"'
ExecStartPre=/bin/sh -lc '/usr/bin/docker pull "$OMNIGRAPH_SERVER_IMAGE"'
ExecStart=/bin/sh -lc 'exec /usr/bin/docker run --name omnigraph-server --rm --network host --env-file ${config_dir}/server.env -e OMNIGRAPH_BIND=0.0.0.0:8080 "$OMNIGRAPH_SERVER_IMAGE"'
ExecStop=/usr/bin/docker stop omnigraph-server
ExecStopPost=-/usr/bin/docker rm -f omnigraph-server
Restart=on-failure
RestartSec=5
TimeoutStartSec=0
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
UNITEOF

systemctl daemon-reload
if [[ "$IMAGE_REF" != *":bootstrap" ]]; then
  systemctl enable omnigraph-server.service
else
  systemctl disable omnigraph-server.service || true
fi

# --- Note: the service is enabled only after a real image reference is configured. ---
echo "Omnigraph infra bootstrap complete. Set ${ssm_image_name}, then enable/start omnigraph-server.service to deploy the runtime container."
