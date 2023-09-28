import stripe
import json
from os import environ
from confluent_kafka import Consumer


conf = {"bootstrap.servers": "kafka:29092", "group.id": "fastapi-kafka-consumer"}
stripe.api_key = environ.get("STRIPE_SECRET_KEY")

consumer = Consumer(conf)
consumer.subscribe(["update-user-on-stripe"])


def update_user(data: dict):
    stripe.Customer.modify(data["id"], email=data["email"], name=data["name"])
    print({"status": "success", "msg": "Updated User"})


try:
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        data = json.loads(msg.value().decode("utf-8"))
        update_user(data)
except Exception as e:
    print({"status": "consumer-update-user-error", "message": str(e)})
finally:
    consumer.close()
