from kafka import KafkaProducer
import json
import time
import random
import numpy as np
from datetime import datetime

# 1. Inicjalizacja producenta
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_finops_event():
    """Generuje event billingowy z logiką anomalii kosztowych."""
    projects = [
        "data-warehouse-prod", "web-backend-dev", "ml-training-env", 
        "sandbox-users", "mobile-api-prod", "ci-cd-pipelines"
    ]
    
    services = [
        ("BigQuery", "Analysis", "Bytes Billed"),
        ("Compute Engine", "N2 Standard Core", "Core Hours"),
        ("Cloud Storage", "Standard Storage", "GB-Months"),
        ("Kubernetes Engine", "GKE Autopilot vCPU", "vCPU Hours"),
        ("Cloud Functions", "Tier 1 Invocations", "Invocations")
    ]
    
    project_id = random.choice(projects)
    service_name, sku_category, usage_unit = random.choice(services)
    
    cost_usd = round(np.random.exponential(scale=5.0) + 0.1, 4)
    if random.random() > 0.95:
        cost_usd = round(random.uniform(301.0, 500.0), 2)

    usage_amount = round(cost_usd * random.uniform(1.5, 3.0), 2)
    
    # Losowy weekend
    is_weekend = np.random.choice([0, 1], p=[5/7, 2/7])
    
    # --- LOGIKA ANOMALII ---
    is_anomaly = 0
    
    if service_name == "BigQuery" and random.random() > 0.8:
        cost_usd = round(random.uniform(200.0, 800.0), 2)
        usage_amount = round(cost_usd * 150, 2)
        is_anomaly = 3
        
    elif is_weekend == 1 and ("dev" in project_id or "sandbox" in project_id):
        if service_name in ["Compute Engine", "Kubernetes Engine"] and cost_usd > 15.0:
            is_anomaly = 2
            cost_usd = round(cost_usd * random.uniform(3.0, 6.0), 2)
            
    elif cost_usd > 300.0:
        is_anomaly = 1

    payload = {
        "billing_record_id": str(random.randint(100000000, 999999999)),
        "project_id": project_id,
        "service": service_name,
        "sku": sku_category,
        "usage_unit": usage_unit,
        "usage_amount": float(usage_amount), 
        "cost_usd": float(cost_usd),        
        "is_weekend": int(is_weekend),   
        "is_anomaly": int(is_anomaly)
    }
    return payload

def main():
    topic_name = 'gcp_billing_events'
    print(f"🚀 Uruchamianie FinOps Data Generatora na topik: '{topic_name}'...")
    
    try:
        event_count = 0
        while True:
            event = generate_finops_event()
            key_bytes = event['project_id'].encode('utf-8')
            
            producer.send(topic_name, key=key_bytes, value=event)
            event_count += 1
            
            print(f"[{event_count}] Wysłano: {event['project_id']} | {event['service']} | Koszt: {event['cost_usd']:.2f} USD")
            
            time.sleep(random.uniform(0.1, 0.5))
            
    except KeyboardInterrupt:
        print("\n⏹️ Przerywam generowanie danych...")
    finally:
        producer.flush()
        producer.close()
        print("🛑 Producent zamknięty.")

if __name__ == '__main__':
    main()
