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
# 1. Creates a Security Group allowing ports 9001-9003 between nodes
# 2. Launches 3 t2.micro EC2 instances (free tier eligible)
# 3. Installs Java 21 + Docker on each
# 4. Copies your project and starts the 3-node cluster
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

# Allow Raft + client ports from anywhere
for PORT in 9001 9002 9003 8080; do
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

  # Install Java 21 + Docker
  ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$ip << EOF
    sudo apt-get update -q
    sudo apt-get install -y -q openjdk-21-jre-headless docker.io maven
    sudo usermod -aG docker ubuntu
EOF

  # Copy project files
  scp -i $KEY_FILE -o StrictHostKeyChecking=no -r \
    ../src ../pom.xml ubuntu@$ip:~/

  # Build and run
  ssh -i $KEY_FILE -o StrictHostKeyChecking=no ubuntu@$ip << EOF
    cd ~
    # Update Main.java peers to use real EC2 IPs
    sed -i 's/127.0.0.1:9002/$peer1/g; s/127.0.0.1:9003/$peer2/g' \
      src/main/java/com/zenith/Main.java 2>/dev/null || true

    mvn clean package -DskipTests --enable-preview -q

    # Run in background, log to file
    nohup java --enable-preview -jar target/zenith-db-1.0-SNAPSHOT.jar \
      $node_name $node_port > ~/zenith.log 2>&1 &

    echo "✅ $node_name started!"
EOF
}

# ── STEP 7: Deploy to all 3 nodes ────────────────────────────
setup_node $IP_A "Node-A" 9001 "$IP_B:9002" "$IP_C:9003"
setup_node $IP_B "Node-B" 9002 "$IP_A:9001" "$IP_C:9003"
setup_node $IP_C "Node-C" 9003 "$IP_A:9001" "$IP_B:9002"

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
echo "  ssh -i $KEY_FILE ubuntu@$IP_A 'tail -f ~/zenith.log'"
echo ""
echo "Node IPs saved to cluster-ips.txt"
echo "Node-A: $IP_A:9001" > cluster-ips.txt
echo "Node-B: $IP_B:9002" >> cluster-ips.txt
echo "Node-C: $IP_C:9003" >> cluster-ips.txt

# ── CLEANUP REMINDER ─────────────────────────────────────────
echo ""
echo "⚠️  IMPORTANT: Run this when done to avoid AWS charges:"
echo "  aws ec2 terminate-instances --instance-ids $INSTANCE_A $INSTANCE_B $INSTANCE_C --region $REGION"