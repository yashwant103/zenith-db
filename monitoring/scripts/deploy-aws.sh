#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# ZenithDB — AWS EC2 Deployment Script
# scripts/deploy-aws.sh
#
# PREREQUISITES:
# 1. AWS CLI installed: brew install awscli (Mac) or apt install awscli (Linux)
# 2. AWS configured: aws configure (enter your Access Key + Secret Key)
# 3. A key pair created in AWS EC2 console → save as zenith-key.pem
# 4. Run: chmod 400 zenith-key.pem
#
# WHAT THIS SCRIPT DOES:
# 1. Creates a Security Group allowing ports 9001-9003 (Raft) and
#    8001-8003 (metrics) between nodes
# 2. Launches 3 t2.micro EC2 instances (free tier eligible)
# 3. Installs Docker on each and builds the ZenithDB image there
# 4. Runs one container per instance with the other two nodes' public
#    IPs passed in as Raft peers (same CLI-arg contract docker-compose.yml
#    uses locally) and a named Docker volume for WAL persistence
#
# COST: ~$0 on free tier (750 hours/month t2.micro free for 12 months)
# If past free tier: ~$0.12/hour for 3 × t2.micro
#
# RUN WITH: bash scripts/deploy-aws.sh
# ═══════════════════════════════════════════════════════════════

set -e  # exit on any error

echo "═══════════════════════════════════════"
echo "  ZenithDB AWS Deployment"
echo "═══════════════════════════════════════"

# ── CONFIGURATION ────────────────────────────────────────────
KEY_NAME="zenith-key"                        # your EC2 key pair name
KEY_FILE="zenith-key.pem"                    # local path to .pem file
AMI_ID="ami-0c7217cdde317cfec"               # Ubuntu 22.04 LTS (us-east-1)
INSTANCE_TYPE="t2.micro"                     # free tier
REGION="us-east-1"
SECURITY_GROUP="zenith-sg"

# ── STEP 1: Create Security Group ────────────────────────────
echo "⚙️  Creating Security Group..."
SG_ID=$(aws ec2 create-security-group \
  --group-name $SECURITY_GROUP \
  --description "ZenithDB Raft cluster security group" \
  --region $REGION \
  --query 'GroupId' \
  --output text 2>/dev/null || \
  aws ec2 describe-security-groups \
    --group-names $SECURITY_GROUP \
    --region $REGION \
    --query 'SecurityGroups[0].GroupId' \
    --output text)

echo "✅ Security Group ID: $SG_ID"

# Allow SSH from anywhere (for deployment)
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID --protocol tcp --port 22 --cidr 0.0.0.0/0 \
  --region $REGION 2>/dev/null || true

# Allow Raft/client ports and each node's actual metrics port.
# NOTE: 8080 was previously opened here, but no node ever listens on 8080 —
# Main.java computes metricsPort = raftPort - 1000, so nodes on 9001/9002/9003
# expose metrics on 8001/8002/8003 respectively (matches docker-compose.yml).
for PORT in 9001 9002 9003 8001 8002 8003; do
  aws ec2 authorize-security-group-ingress \
    --group-id $SG_ID --protocol tcp --port $PORT --cidr 0.0.0.0/0 \
    --region $REGION 2>/dev/null || true
done

echo "✅ Security Group configured"

# ── STEP 2: Launch 3 EC2 instances ───────────────────────────
echo "🚀 Launching 3 EC2 instances..."

launch_instance() {
  local name=$1
  aws ec2 run-instances \
    --image-id $AMI_ID \
    --instance-type $INSTANCE_TYPE \
    --key-name $KEY_NAME \
    --security-group-ids $SG_ID \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=zenith-$name}]" \
    --region $REGION \
    --query 'Instances[0].InstanceId' \
    --output text
}

INSTANCE_A=$(launch_instance "node-a")
INSTANCE_B=$(launch_instance "node-b")
INSTANCE_C=$(launch_instance "node-c")

echo "✅ Instance A: $INSTANCE_A"
echo "✅ Instance B: $INSTANCE_B"
echo "✅ Instance C: $INSTANCE_C"

# ── STEP 3: Wait for instances to start ──────────────────────
echo "⏳ Waiting for instances to be running..."
aws ec2 wait instance-running \
  --instance-ids $INSTANCE_A $INSTANCE_B $INSTANCE_C \
  --region $REGION
echo "✅ All instances running"

# ── STEP 4: Get public IPs ────────────────────────────────────
get_ip() {
  aws ec2 describe-instances \
    --instance-ids $1 \
    --region $REGION \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text
}

IP_A=$(get_ip $INSTANCE_A)
IP_B=$(get_ip $INSTANCE_B)
IP_C=$(get_ip $INSTANCE_C)

echo "🌐 Node-A IP: $IP_A"
echo "🌐 Node-B IP: $IP_B"
echo "🌐 Node-C IP: $IP_C"

# ── STEP 5: Wait for SSH to be available ─────────────────────
echo "⏳ Waiting for SSH to be ready (60 seconds)..."
sleep 60

# ── STEP 6: Setup function ────────────────────────────────────
setup_node() {
  local ip=$1
  local node_name=$2
  local node_port=$3
  local peer1=$4
  local peer2=$5

  echo "⚙️  Setting up $node_name at $ip..."

  # Install Docker only — the app runs inside the container, so the host
  # doesn't need a JDK or Maven at all.
  ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$ip << 'EOF'
    sudo apt-get update -q
    sudo apt-get install -y -q docker.io
    sudo usermod -aG docker ubuntu
EOF

  # Copy everything the Dockerfile needs to build the image
  scp -i $KEY_FILE -o StrictHostKeyChecking=no -r \
    ../src ../pom.xml ../Dockerfile ubuntu@$ip:~/

  # FIX: previously this ran `mvn package` + bare `java -jar` directly on the
  # EC2 host and patched peer IPs into Main.java via a sed text substitution —
  # fragile (breaks silently if the source strings ever change) and it never
  # actually used the Docker install above, contradicting the project's whole
  # containerized design. Peers are passed as real CLI args instead — the
  # same mechanism docker-compose.yml already uses locally — and the app
  # builds/runs exactly as it does in Docker Compose.
  ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$ip << EOF
    cd ~
    sudo docker build -t zenith-db .
    sudo docker volume create zenith-data
    sudo docker run -d --name zenith \
      --restart unless-stopped \
      -p ${node_port}:${node_port} \
      -p $((node_port - 1000)):$((node_port - 1000)) \
      -v zenith-data:/data \
      zenith-db $node_name $node_port $peer1 $peer2

    echo "✅ $node_name started!"
EOF
}

# ── STEP 7: Deploy to all 3 nodes ────────────────────────────
setup_node $IP_A "node-a" 9001 "$IP_B:9002" "$IP_C:9003"
setup_node $IP_B "node-b" 9002 "$IP_A:9001" "$IP_C:9003"
setup_node $IP_C "node-c" 9003 "$IP_A:9001" "$IP_B:9002"

# ── DONE ─────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════"
echo "✅ ZenithDB 3-node cluster deployed!"
echo "═══════════════════════════════════════"
echo ""
echo "Connect to cluster:"
echo "  echo 'INSERT,T1,AAPL,100,150.0,PENDING' | nc $IP_A 9001"
echo "  echo 'SELECT,T1'                         | nc $IP_A 9001"
echo ""
echo "View logs:"
echo "  ssh -i $KEY_FILE ubuntu@$IP_A 'sudo docker logs -f zenith'"
echo ""
echo "Node IPs saved to cluster-ips.txt"
echo "Node-A: $IP_A:9001" > cluster-ips.txt
echo "Node-B: $IP_B:9002" >> cluster-ips.txt
echo "Node-C: $IP_C:9003" >> cluster-ips.txt

# ── CLEANUP REMINDER ─────────────────────────────────────────
echo ""
echo "⚠️  IMPORTANT: Run this when done to avoid AWS charges:"
echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_A $INSTANCE_B $INSTANCE_C --region $REGION"