from kafka_app.consumer import BatchMessageConsumer


# Run BatchMessageConsumer to consume messages in batch mode
consumer_batch = BatchMessageConsumer(
    bootstrap_servers="localhost:9094",
    group_id="energy-group-batch",
    topics=["energy-consumption"],
    auto_commit=False
)

try:
    while True:
        batch = consumer_batch.consume_batch(batch_size=10, timeout=1.0)

        if not batch:
            continue

        print(
            f"Batch processed successfully. "
            f"Messages count: {len(batch)}"
        )

finally:
    consumer_batch.close()