from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

for message in consumer:
    tx = message.value
    hour = tx.get('hour', int(tx['timestamp'][11:13]))
    is_elec = 1 if tx.get('category') == 'elektronika' else tx.get('is_electronics', 0)

    features = {
        "amount": tx['amount'],
        "hour": hour,
        "is_electronics": is_elec,
        "tx_per_day": 5
    }

    try:
        r = requests.post(API_URL, json=features)
        result = r.json()

        if result['is_fraud']:
            tx['ml_score'] = result['fraud_probability']
            alert_producer.send('alerts', value=tx)
            print(f"🚨 FRAUD | prob={result['fraud_probability']:.2f} | {tx['tx_id']} | {tx['amount']:.2f} PLN")
        else:
            print(f"   OK    | prob={result['fraud_probability']:.2f} | {tx['tx_id']} | {tx['amount']:.2f} PLN")
    except Exception as e:
        print(f"Błąd API: {e}")
