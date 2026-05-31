from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def score_transaction(tx):
    score = 0
    rules = []
    
    if tx['amount'] > 3000:
        score += 3
        rules.append("R1")
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append("R2")
    if tx.get('hour', 12) < 6:
        score += 2
        rules.append("R3")
        
    return score, rules

for message in consumer:
    tx = message.value
    score, triggered_rules = score_transaction(tx)
    
    if score >= 3:
        tx['fraud_score'] = score
        tx['rules'] = triggered_rules
        alert_producer.send('alerts', value=tx)
        print(f"🚨 FRAUD ALERT! ID: {tx['tx_id']} | Score: {score} | Rules: {triggered_rules}")
