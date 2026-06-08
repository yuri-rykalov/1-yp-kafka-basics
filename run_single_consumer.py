from kafka_app.consumer import SingleMessageConsumer

# Run SingleMessageConsumer to consume messages in single mode
consumer_single = SingleMessageConsumer(
    bootstrap_servers="localhost:9094",
    group_id="energy-group-single",
    topics=["energy-consumption"],
    auto_commit=True
)

try:
    while True:
        consumer_single.consume_one()

finally:
    consumer_single.close()
