from kafka_app.producer import KafkaProducer
from kafka_app.consumer import SingleMessageConsumer
from utils.energy_payload import EnergyConsumption

# 1 - Produce messages
producer = KafkaProducer("localhost:9094")

ec = EnergyConsumption()

for i in range(10):
    producer.send(
        topic="energy-consumption",
        key=f"ec-{i}",
        value=ec.energy_cons_payload()
    )

producer.flush()

# 2 - Consume messages via SingleMessageConsumer
consumer_single = SingleMessageConsumer(
    bootstrap_servers="localhost:9094",
    group_id="energy-group",
    topics=["energy-consumption"],
    auto_commit=True
)

try:
    while True:
        consumer_single.consume_one()

finally:
    consumer_single.close()