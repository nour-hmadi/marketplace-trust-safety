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
CUSTOMER_IDS = ['C71', 'C72', 'C73', 'C74', 'C75']
CATEGORIES = ['electronics', 'clothing', 'home_goods', 'luxury']
STATUSES = ['completed', 'pending', 'cancelled']

def generate_order():
    return {
        "order_id": f"O{random.randint(100, 999)}",
        "seller_id": random.choice(SELLER_IDS),
        "customer_id": random.choice(CUSTOMER_IDS),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.now().isoformat(),
        "status": random.choice(STATUSES),
        "category": random.choice(CATEGORIES)
    }

print("Starting order producer...")

while True:
    order = generate_order()
    producer.send('orders', value=order)
    print(f"Sent order: {order}")
    time.sleep(2)