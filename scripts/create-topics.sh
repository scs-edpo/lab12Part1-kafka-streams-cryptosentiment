echo "Waiting for Kafka to come online..."
cub kafka-ready -b 'kafka:9092' 1 20
# create the tweets topic
kafka-topics --create --bootstrap-server 'kafka:9092' --topic 'tweets' --replication-factor '1' --partitions '1'
# create the crypto-sentiment topic
kafka-topics --create --bootstrap-server 'kafka:9092' --topic 'crypto-sentiment' --replication-factor '1' --partitions '1'
sleep infinity
