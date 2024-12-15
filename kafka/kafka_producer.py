from kafka import KafkaProducer
import pandas as pd
import json

# Charger les données prétraitées (ou les données originales si non prétraitées)
file_path = "data/cleaned_weatherAUS.csv"  # Changez vers weatherAUS.csv si nécessaire
data = pd.read_csv(file_path)

# Initialiser le Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convertir les messages en JSON
)

# Nom du topic Kafka
topic_name = "rain-prediction"

# Envoyer les données ligne par ligne
for _, row in data.iterrows():
    producer.send(topic_name, value=row.to_dict())  # Convertir la ligne en dictionnaire avant envoi
    print(f"Message envoyé : {row.to_dict()}")  # Optionnel : log des messages
   
print("Tous les messages ont été envoyés au topic Kafka avec succès.")
producer.close()
