import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

SELLER_IDS = ['S55', 'S56', 'S57', 'S58', 'S59']
REASONS = ['damaged_item', 'wrong_item', 'not_as_described', 'changed_mind', 'counterfeit']
STATUSES = ['approved', 'pending', 'rejected']

def generate_return():
    return {
        "return_id": f"R{random.randint(100, 999)}",
        "order_id": f"O{random.randint(100, 999)}",
        "seller_id": random.choice(SELLER_IDS),
        "timestamp": datetime.now().isoformat(),
        "reason": random.choice(REASONS),
        "status": random.choice(STATUSES)
    }

print("Starting return producer...")

while True:
    return_event = generate_return()
    producer.send('returns', value=return_event)
    print(f"Sent return: {return_event}")
    time.sleep(3)