import json
from confluent_kafka import Consumer, KafkaError, KafkaException

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-fulfillment-group",
    "auto.offset.reset": "earliest" 
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])
print(" Listening for orders...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        key = msg.key().decode("utf-8") if msg.key() else "No Key"
        
        order_data = json.loads(msg.value().decode("utf-8"))

        print(f" New Order Received (Partition: {msg.partition()})")
        print(f" User: {key}")
        print(f" Item: {order_data['quantity']}x {order_data['item']}")
        print(f" Order ID: {order_data['order_id']}\n")

except KeyboardInterrupt:
    print("\n Shutting down consumer...")
finally:
    consumer.close()
