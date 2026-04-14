from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value
    amount = tx['amount']

    if amount > 3000:
        tx['risk_level'] = "HIGH"
    elif amount > 1000:
        tx['risk_level'] = "MEDIUM"
    else:
        tx['risk_level'] = "LOW"
    
    if amount > 1000:
        print(f"!!! ALERT [{tx['risk_level']}] !!! ID: {tx['tx_id']} | Kwota: {amount} PLN")
