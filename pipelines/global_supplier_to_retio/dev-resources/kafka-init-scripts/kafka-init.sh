kafka-topics --bootstrap-server kafka:29092 --list
topics='["leyland_topic", "australia_topic"]'

echo "Creating kafka topics...{$topics}"

echo "$topics" | tr -d '[]"' | tr ',' '\n' | sed 's/^ //' | while read topic; do
  kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done

echo 'Successfully created the following topics:'
kafka-topics --bootstrap-server kafka:29092 --list