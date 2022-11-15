from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('alienvaultdata', bootstrap_servers='broker:9092')

print("Printing messages on alienvaultdata")
for msg in consumer:
    print(json.loads(msg.value))
print("Done")
