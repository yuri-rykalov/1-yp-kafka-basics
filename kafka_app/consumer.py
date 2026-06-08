import json
from confluent_kafka import Consumer


class KafkaConsumer:

    def __init__(
            self, 
            bootstrap_servers, 
            group_id, 
            topics, 
            auto_offset="earliest", 
            auto_commit=True
    ):
        
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers, # Kafka broker address
            "group.id": group_id,                   # Group ID
            "auto.offset.reset": auto_offset,       # If no offset - read from beginning
            "enable.auto.commit": auto_commit,      # Auto commit of offsets
            "session.timeout.ms": 6_000             # Timeout setting - 6 seconds
        })

        self.consumer.subscribe(topics)

    def deserialize_json(self, message_value):
        """
        Reads message
        Deserializes it in JSON
        """

        try:
            return json.loads(message_value.decode("utf-8"))
        
        except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as e:
            print(f"❌ Deserialization error")
            print(f"Raw message: {message_value}")
            print(f"Error: {e}")
            return None
        
    def close(self):
        self.consumer.close()


class SingleMessageConsumer(KafkaConsumer):

    def consume_one(self, timeout=1.0):
        """
        Reads Kafka messages one by one
        Auto commits offset
        """

        msg = self.consumer.poll(timeout)

        # If there are no messages
        if msg is None:
            return None
        
        # Kafka-level error
        if msg.error():
            print(f"❌ Kafka consumer error: {msg.error()}")
            return None
        
        # Read, deserialize and print message
        payload = self.deserialize_json(msg.value())

        if payload is None:
            return None
        
        print(
            f"✅ Received message from "
            f"{msg.topic()} [{msg.partition()}] "
            f"offset={msg.offset()}"
        )
        print(f"Payload: {payload}")

        return payload
    

class BatchMessageConsumer(KafkaConsumer):
    pass
