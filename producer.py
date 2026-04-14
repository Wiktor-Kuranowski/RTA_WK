from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def generate_transaction():
    # 1. Losujemy bazowe parametry dla każdej transakcji
    tx = {
        'tx_id': f'TX{random.randint(1000,9999)}',
        'user_id': f'u{random.randint(1,20):02d}',
        'store': random.choice(sklepy),
        'timestamp': datetime.now().isoformat(),
    }

    # 2. Mechanizm losowania: 95% to zwykłe zakupy, 5% to "szaleństwo"
    if random.random() > 0.05:
        # ZWYKŁA TRANSAKCJA
        tx['amount'] = round(random.uniform(10.0, 1500.0), 2) # Niskie kwoty
        tx['category'] = random.choice(kategorie)
        tx['hour'] = random.randint(6, 23) # Dzień
    else:
        # PODEJRZANA TRANSAKCJA (ale nie zawsze na 7 punktów!)
        # Tutaj mieszamy parametry, żeby raz wpadało R1, a raz R3
        tx['amount'] = round(random.uniform(1000.0, 5000.0), 2)
        tx['category'] = random.choice(['elektronika', 'żywność']) # Czasem elektronika, czasem nie
        tx['hour'] = random.choice([2, 3, 4, 14, 15]) # Czasem noc, czasem dzień
    
    return tx

for i in range(1000):
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    print(f"[{i+1}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
    time.sleep(0.5)

producer.flush()
producer.close()
