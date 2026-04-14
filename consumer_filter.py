%%file consumer_filter.py
from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', # Czytaj od początku tematu
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


for message in consumer:
  
    tx = message.value
    amount = tx.get('amount', 0)
    
    if amount > 1000:
        print(f"🚨 ALERT: Wykryto dużą transakcję! ID: {tx['tx_id']} | Kwota: {amount:.2f} PLN | Sklep: {tx['store']}")
    else:
        print(f"   [INFO] Transakcja {tx['tx_id']} poniżej limitu ({amount:.2f} PLN)")