# create_emulator_topics.py
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound, AlreadyExists
from common.constants import PROJECT_ID, PUBSUB_TOPIC_DASHBOARD_UPDATES, PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED, \
    PUBSUB_TOPIC_REPORT_GENERATION_REQUESTS
import os


def create_topic_if_not_exists(publisher, project_id, topic_id):
    topic_path = publisher.topic_path(project_id, topic_id)
    try:
        # Try to get the topic to check if it exists
        publisher.get_topic(request={"topic": topic_path})
        print(f"Topic '{topic_id}' already exists in the emulator (from get_topic check).")
    except NotFound:
        # Topic does not exist, so try to create it
        print(f"Topic '{topic_id}' not found. Attempting to create...")
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"SUCCESS: Topic '{topic_id}' created successfully in the emulator.")
        except AlreadyExists:
            print(f"Topic '{topic_id}' already exists in the emulator (was created concurrently).")
        except Exception as create_e:
            print(f"FAILED to create topic '{topic_id}' in emulator: {create_e}")
    except Exception as e:
        # Catch any other unexpected errors during the get_topic call
        print(f"An unexpected error occurred while checking topic '{topic_id}': {e}")


if __name__ == "__main__":
    emulator_host = os.environ.get('PUBSUB_EMULATOR_HOST')
    if not emulator_host:
        print("ERROR: PUBSUB_EMULATOR_HOST environment variable is not set. Please set it to '127.0.0.1:8085'.")
        exit(1)

    print(f"Attempting to create topics in Pub/Sub Emulator at {emulator_host}")
    publisher = pubsub_v1.PublisherClient()

    topics_to_create = [
        PUBSUB_TOPIC_DASHBOARD_UPDATES,
        PUBSUB_TOPIC_REPORT_GENERATION_COMPLETED,
        PUBSUB_TOPIC_REPORT_GENERATION_REQUESTS
    ]

    for topic_name in topics_to_create:
        create_topic_if_not_exists(publisher, PROJECT_ID, topic_name)

    print("\nTopic creation process completed.")