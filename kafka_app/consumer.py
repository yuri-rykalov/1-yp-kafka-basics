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

        # Receive 1 message
        msg = self.consumer.poll(timeout)

        # If there is no message
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
    
    def consume_batch(self, batch_size=10, timeout=1.0):
        """
        Reads Kafka messages as a batch (by default 10 in a batch)
        Processes messages in the cycle: deserializes message, prints it, if deserialization failed - prints error
        Commits offset manually, one time after all the messages in a batch have been processed
        """

        # Receive batch of messages
        messages = self.consumer.consume(
            num_messages = batch_size, 
            timeout = timeout
        )

        # If there is no batch
        if not messages:
            return []
        
        processed_payloads = []   # Declare list for processed payloads

        # Process messages in the cycle
        for i, msg in enumerate(messages):
            if msg is None:
                continue
            
            # Kafka-level error
            if msg.error():
                print(f"❌ Kafka consumer error: {msg.error()}")
                continue
            
            # Read, deserialize and print message
            payload = self.deserialize_json(msg.value())

            if payload is None:
                continue
            
            print(
                f"✅ Received message {i + 1} of batch len: {len(messages)} from "
                f"{msg.topic()} [{msg.partition()}] "
                f"offset={msg.offset()}"
            )
            print(f"Payload: {payload}")

            # Add payload to the list
            processed_payloads.append(payload)
        
        self.consumer.commit(asynchronous=False)
        return processed_payloads


