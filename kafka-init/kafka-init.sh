kafka-topics --bootstrap-server kafka:29092 --list

echo 'Creating kafka topics...'
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic input_topic --partitions 1 --replication-factor 1
echo 'Creating kafka topics...'
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic output_topic --partitions 1 --replication-factor 1
echo 'Successfully created the following topics:'
kafka-topics --bootstrap-server kafka:29092 --list