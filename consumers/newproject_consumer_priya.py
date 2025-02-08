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
# Stylish Real-Time Bar Chart Function
#####################################

def stylish_anime():
    """Animates a real-time bar chart with a modern visual style."""
    plt.style.use("seaborn-dark")  # Apply a dark theme

    def update_chart(frame):
        plt.cla()  # Clear previous chart

        categories = list(message_counts.keys())
        counts = list(message_counts.values())

        # Define unique colors for categories
        colors = ['#FF6F61', '#6B5B95', '#88B04B', '#92A8D1', '#F7CAC9', '#955251', '#B565A7'][:len(categories)]

        # Create bar chart with enhanced styles
        bars = plt.bar(categories, counts, color=colors, edgecolor="black", linewidth=1.2)

        # Add background color
        plt.gca().set_facecolor("#2E2E2E")  # Dark gray background
        plt.figure(figsize=(10, 5))

        # Customize grid
        plt.grid(axis='y', linestyle='--', alpha=0.5, color='white')

        # Set labels and title with stylish font and colors
        plt.xlabel("Categories", fontsize=12, fontweight="bold", color="white")
        plt.ylabel("Total Messages", fontsize=12, fontweight="bold", color="white")
        plt.title("🔥 Real-Time Kafka Message Count by Category", fontsize=14, fontweight="bold", color="#FFDD44")

        # Rotate x-axis labels
        plt.xticks(rotation=45, fontsize=10, color="white")
        plt.yticks(fontsize=10, color="white")

        # Annotate bars with values
        for bar in bars:
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, str(bar.get_height()),
                     ha='center', fontsize=10, fontweight="bold", color="white")

    # Start animation
    fig = plt.figure(facecolor="#222222")  # Set figure background color
    ani = FuncAnimation(fig, update_chart, interval=1000, cache_frame_data=False)
    plt.show()

#####################################
# Main Execution
#####################################

if __name__ == "__main__":
    # Start Kafka consumer in a background thread
    threading.Thread(target=consume_messages, daemon=True).start()

    # Run the stylish live chart
    stylish_anime()
