from kafka_app.producer import KafkaProducer
from utils.energy_payload import EnergyConsumption


# Run Producer to produce messages
producer = KafkaProducer("localhost:9094")

ec = EnergyConsumption()

for i in range(10):
    producer.send(
        topic="energy-consumption",
        key=f"ec-{i}",
        value=ec.energy_cons_payload()
    )

producer.flush()