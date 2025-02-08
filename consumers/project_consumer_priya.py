import json
import threading
from collections import defaultdict
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

#####################################
# Kafka Configuration
#####################################

TOPIC = "project_json"
KAFKA_SERVER = "localhost:9092"

#####################################
# Data Storage
#####################################

message_counts = defaultdict(int)  # Store total message counts per category

#####################################
# Kafka Consumer Function
#####################################

def consume_messages():
    """Consumes messages from Kafka and updates category message counts."""
    print("📡 Waiting for messages from Kafka...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for message in consumer:
        data = message.value
        category = data.get("category", "other")

        # Increment the count for this category
        message_counts[category] += 1
        print(f"📊 Updated Message Counts: {message_counts}")

#####################################
# Real-Time Visualization Function
#####################################

def anime():
    """Animates a live bar chart showing message counts per category."""
    def update_chart(frame):
        plt.cla()  # Clear previous chart

        categories = list(message_counts.keys())
        counts = list(message_counts.values())

        # Define unique colors for categories
        colors = ['blue', 'green', 'red', 'purple', 'orange', 'cyan', 'pink'][:len(categories)]
        
        # Create bar chart
        plt.bar(categories, counts, color=colors)
        plt.xlabel("Lifestyles-Expenses")
        plt.ylabel("Total Expense for lifesyle")
        plt.title("🔥 Real-Time Kafka Expense Count by Lifestyle")
        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='-', alpha=0.7)

    # Start animation
    fig = plt.figure()
    ani = FuncAnimation(fig, update_chart, interval=1000, cache_frame_data=False)
    plt.show()

#####################################
# Main Execution
#####################################

if __name__ == "__main__":
    # Start Kafka consumer in a background thread
    threading.Thread(target=consume_messages, daemon=True).start()

    # Run the live chart
    # Run the stylish live chart
    anime()
