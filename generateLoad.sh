# Install s3cmd or aws-cli if not already present
pip install awscli --break-system-packages 2>/dev/null || yum install -y awscli

# Configure for zone1 (master)
aws configure set aws_access_key_id ${MASTER_ACCESS_KEY}
aws configure set aws_secret_access_key ${MASTER_SECRET_KEY}

# Create test buckets on master zone
for i in 1 2 3 4 5; do
  aws --endpoint-url=http://${NODE1}:${MASTER_PORT} s3 mb s3://test-bucket-${i}
done

# Upload some test objects
for i in 1 2 3 4 5; do
  dd if=/dev/urandom bs=1K count=100 2>/dev/null | \
    aws --endpoint-url=http://${NODE1}:${MASTER_PORT} \
    s3 cp - s3://test-bucket-${i}/testfile-${i}.dat
done

echo "Waiting 10s for sync..."
sleep 10

# Verify on secondary zone
for i in 1 2 3 4 5; do
  echo -n "test-bucket-${i} on zone2: "
  aws --endpoint-url=http://${NODE2}:${SECONDARY_PORT} \
    s3 ls s3://test-bucket-${i}/ 2>/dev/null | wc -l
done