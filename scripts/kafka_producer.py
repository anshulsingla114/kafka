from kafka import KafkaProducer
import json
import time
import random

# Kafka broker connection settings
bootstrap_servers = ['localhost:9092']
topic_name = 'user_activities'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Simulate user activities
def simulate_user_activity():
    while True:
        # Simulate random user activity
        user_id = random.randint(1, 1000)
        activity_type = random.choice(['click', 'page_view', 'purchase'])
        timestamp = int(time.time())

        # Prepare message
        message = {
            'user_id': user_id,
            'activity_type': activity_type,
            'timestamp': timestamp
        }

        # Send message to Kafka
        producer.send(topic_name, value=message)
        print(f"Sent message: {message}")

        # Wait for a random interval (simulate real-time activity)
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    try:
        simulate_user_activity()
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        # Ensure all messages are sent before exiting
        producer.flush()
        producer.close()
