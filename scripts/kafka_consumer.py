from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
conn = psycopg2.connect(
    dbname="user_activity_db",
    user="YOUR USER NAME",
    password="YOUR PASSWORD",
    host="localhost"
)
cursor = conn.cursor()

# Kafka consumer configuration
consumer = KafkaConsumer(
    'user_activities',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mygroup',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process messages
for message in consumer:
    user_id = message.value.get('user_id')
    activity_type = message.value.get('activity_type')
    timestamp = message.value.get('timestamp')

    # Insert data into PostgreSQL
    insert_query = sql.SQL(
        "INSERT INTO activities (user_id, action, timestamp) VALUES (%s, %s, to_timestamp(%s))"
    )
    cursor.execute(insert_query, (user_id, activity_type, timestamp))
    conn.commit()

    print(f'Received and stored message: {message.value}')

# Close PostgreSQL connection
cursor.close()
conn.close()
