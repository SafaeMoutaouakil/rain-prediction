from kafka import KafkaConsumer
import json

# Initialiser le Kafka Consumer
consumer = KafkaConsumer(
    'rain-prediction',  # Nom du topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Lire les messages depuis le début
    group_id='rain-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Désérialiser les messages JSON
)

# Lire les messages
print("Lecture des messages depuis le topic 'rain-prediction' :")
for message in consumer:
    print(f"Message reçu : {message.value}")
