from kafka import KafkaConsumer, KafkaProducer
import json, requests

BROKER = "broker:9092"
API_URL = "http://localhost:8002/score" 

consumer = KafkaConsumer(
    'gcp_billing_events',
    bootstrap_servers=BROKER,
    group_id='ml-scoring-group',          
    auto_offset_reset='earliest',        
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("✅ Konsument uruchomiony i nasłuchuje na 'gcp_billing_events'...")

try:
    for message in consumer:
        tx = message.value
        
        # --- MAPOWANIE ---
        features = {
            "billing_record_id": tx.get('billing_record_id', 'unknown'),
            "project_id": tx.get('project_id', 'unknown'),
            "service": tx.get('service', 'unknown'),
            "sku": tx.get('sku', 'unknown'),
            "usage_unit": tx.get('usage_unit', 'unknown'),
            "usage_amount": float(tx.get('usage_amount', 0)),
            "cost_usd": float(tx.get('cost_usd', 0)),
            "is_weekend": int(tx.get('is_weekend', 0)),
            "is_anomaly": int(tx.get('is_anomaly', 0))
        }
        
     
        try:
            response = requests.post(API_URL, json=features, timeout=2)
            result = response.json()
        except requests.RequestException:
            continue

        if result.get('is_fraud'):
            service = tx.get('service', 'Unknown')
            cost = tx.get('cost_usd', 0.0)
            prob = result.get('fraud_probability', 0.0)
            tx_id = tx.get('billing_record_id', 'N/A')

          
            print(f"🚨 [FRAUD DETECTED] ID: {tx_id} | Serwis: {service} | Koszt: {cost} USD | Anomaly Score: {prob:.4f}")
            
           
            alert = {
                **tx,
                'fraud_probability': prob,
                'alert_source': 'ml_model'
            }
            alert_producer.send('alerts', value=alert)

except KeyboardInterrupt:
    print("Zamykanie...")
finally:
    alert_producer.flush()
    alert_producer.close()
    consumer.close()