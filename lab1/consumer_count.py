from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    store_counts[store] += 1
    total_amount[store] += tx['amount']
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n{'='*50}")
        print(f"  PODSUMOWANIE po {msg_count} transakcjach")
        print(f"{'='*50}")
        for s in sorted(store_counts):
            print(f"  {s:12s} | {store_counts[s]:4d} tx | {total_amount[s]:10.2f} PLN")
        print(f"{'='*50}")
