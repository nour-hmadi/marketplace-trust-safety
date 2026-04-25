import json
import time
import random
from datetime import datetime

messages = [
    {"order_id": "O9999", "seller_id": "S58", "customer_id": "C150", "amount": 450.0, "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "status": "completed", "category": "electronics"},
    {"order_id": "O9998", "seller_id": "S61", "customer_id": "C160", "amount": 230.0, "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "status": "completed", "category": "clothing"},
    {"order_id": "O9997", "seller_id": "S62", "customer_id": "C170", "amount": 1200.0, "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "status": "completed", "category": "luxury"},
    {"order_id": "O9996", "seller_id": "S58", "customer_id": "C180", "amount": 800.0, "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "status": "completed", "category": "electronics"},
    {"order_id": "O9995", "seller_id": "S59", "customer_id": "C190", "amount": 2000.0, "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "status": "completed", "category": "luxury"},
]

try:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for msg in messages:
        producer.send('orders', value=msg)
        print(f"✅ Sent order: {msg['order_id']} | Seller: {msg['seller_id']} | Amount: ${msg['amount']}")
        time.sleep(1)
    producer.flush()
    print("\n🎉 All messages sent to Kafka!")

except Exception as e:
    print(f"Kafka producer error: {e}")
    print("\nShowing messages that would be sent:")
    for msg in messages:
        print(f"  Order: {msg['order_id']} | Seller: {msg['seller_id']} | Amount: ${msg['amount']}")
