
import json
improt uuid
from confluent_kafka import Producer

producer_config = {
  "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
  if err: 
    print(f"Delivery failed: {err}");
  else:
    key = msg.key().decode('utf-8') if msg.key() else "None"
    print(f" Delivered order for {key}")
    print(f" Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")

order = {
  "order_id": str(uuid.uuid4()),
  "user": "lara",
  "item": "frozen yogurt",
  "quantity": 10
}

value = json.dump(order).encode('utf-8')
key = order["user"].encode('utf-8')

producer.produce(
  topic="orders",
  key=key,
  value=value,
  callback=delivery_report
)

producer.flush()
