from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import random
from datetime import datetime

app = Flask(__name__)
CORS(app)

events_log = []

def try_kafka(topic, message):
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=3000,
            api_version=(0, 10, 1)
        )
        producer.send(topic, value=message)
        producer.flush()
        producer.close()
        return True
    except Exception as e:
        print(f"Kafka not available: {e}")
        return False

@app.route('/api/order', methods=['POST'])
def send_order():
    data = request.json
    order = {
        "order_id": f"O_LIVE_{random.randint(10000,99999)}",
        "seller_id": data.get('seller_id'),
        "customer_id": f"C_{random.randint(100,999)}",
        "amount": float(data.get('amount', 0)),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "status": data.get('status', 'completed'),
        "category": data.get('category', 'electronics')
    }
    kafka_ok = try_kafka('orders', order)
    events_log.append({"type": "ORDER", "data": order, "kafka": kafka_ok})
    return jsonify({"success": True, "event": order, "kafka_sent": kafka_ok})

@app.route('/api/return', methods=['POST'])
def send_return():
    data = request.json
    ret = {
        "return_id": f"R_LIVE_{random.randint(10000,99999)}",
        "order_id": f"O_{random.randint(1000,9999)}",
        "seller_id": data.get('seller_id'),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "reason": data.get('reason', 'damaged_item'),
        "status": "approved"
    }
    kafka_ok = try_kafka('returns', ret)
    events_log.append({"type": "RETURN", "data": ret, "kafka": kafka_ok})
    return jsonify({"success": True, "event": ret, "kafka_sent": kafka_ok})

@app.route('/api/complaint', methods=['POST'])
def send_complaint():
    data = request.json
    complaint = {
        "complaint_id": f"CMP_LIVE_{random.randint(10000,99999)}",
        "seller_id": data.get('seller_id'),
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "severity": data.get('severity', 'medium'),
        "category": data.get('category', 'poor_quality')
    }
    kafka_ok = try_kafka('complaints', complaint)
    events_log.append({"type": "COMPLAINT", "data": complaint, "kafka": kafka_ok})
    return jsonify({"success": True, "event": complaint, "kafka_sent": kafka_ok})

@app.route('/api/events', methods=['GET'])
def get_events():
    return jsonify({"events": events_log[-10:]})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify({
        "total": len(events_log),
        "orders": len([e for e in events_log if e['type'] == 'ORDER']),
        "returns": len([e for e in events_log if e['type'] == 'RETURN']),
        "complaints": len([e for e in events_log if e['type'] == 'COMPLAINT']),
    })

if __name__ == '__main__':
    print("Starting Marketplace Event Producer API on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=False)
