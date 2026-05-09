from kafka import KafkaConsumer, KafkaProducer
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

for message in consumer:
    tx = message.value
    is_elec = 1 if tx.get('category') == 'elektronika' else 0
    features = {"amount": tx['amount'], "is_electronics": is_elec, "tx_per_minute": 5}

    try:
        r = requests.post(API_URL, json=features, timeout=2)
        result = r.json()
        if result['is_fraud']:
            tx['fraud_probability'] = result['fraud_probability']
            tx['alert_source'] = 'ml_model'
            alert_producer.send('alerts', value=tx)
            print(f"🚨 FRAUD [{result['fraud_probability']:.0%}] {tx['tx_id']} | {tx['amount']:.2f} PLN")
            alert_producer.flush()
        else:
            print(f"   OK    | {tx['tx_id']} | {tx['amount']:.2f} PLN")
    except Exception as e:
        print(f"Błąd API: {e}")
