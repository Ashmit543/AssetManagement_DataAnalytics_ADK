# common/adk_base.py

import json
import os
import base64 # Make sure this line is present and NOT commented out
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
from common.constants import PROJECT_ID, PUBSUB_TOPIC_DASHBOARD_UPDATES, REGION # REGION might not be needed here if not directly used, but harmless for now

class ADKBaseAgent:
    """
    Base class for all Agents in the Asset Data Kit.
    Handles Flask app setup, Pub/Sub message reception, and message publishing.
    """
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.app = Flask(agent_name)
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = PROJECT_ID

        # Register the Pub/Sub push endpoint
        @self.app.route('/', methods=['POST'])
        def index():
            return self.handle_pubsub_message(request)

        print(f"{self.agent_name} initialized.")

    def handle_pubsub_message(self, request):
        """
        Receives and processes Pub/Sub push messages.
        """
        if request.method != 'POST':
            return 'OK', 200 # Acknowledge non-POST requests

        # Ensure the request body is valid JSON
        if not request.is_json:
            print(f"{self.agent_name}: Invalid request, must be JSON.")
            return 'Bad Request: not JSON', 400

        envelope = request.get_json()
        if not envelope:
            print(f"{self.agent_name}: Invalid Pub/Sub message format (missing envelope).")
            return 'Bad Request: missing envelope', 400

        if not isinstance(envelope, dict) or 'message' not in envelope:
            print(f"{self.agent_name}: Invalid Pub/Sub message format (missing 'message' key).")
            return 'Bad Request: malformed message', 400

        message = envelope.get('message')
        if not isinstance(message, dict) or 'data' not in message:
            print(f"{self.agent_name}: Invalid Pub/Sub message format (missing 'data' in message).")
            return 'Bad Request: missing data', 400

        try:
            # Pub/Sub message data is base64 encoded
            decoded_data = base64.b64decode(message['data']).decode('utf-8')
            message_data = json.loads(decoded_data)
            print(f"[{self.agent_name}] Received message: {message_data}")

            # Process the message
            self.process_message(message_data)

            return 'OK', 200
        except Exception as e:
            print(f"[{self.agent_name}] Error processing message: {e}")
            return 'Internal Server Error', 500

    def process_message(self, message_data: dict):
        """
        Abstract method to be implemented by subclass for specific message processing.
        """
        raise NotImplementedError("Subclasses must implement process_message method.")

    def publish_message(self, topic_id: str, data: dict):
        """
        Publishes a message to a specified Pub/Sub topic.
        """
        topic_path = self.publisher.topic_path(self.project_id, topic_id)
        data_bytes = json.dumps(data).encode('utf-8')

        try:
            future = self.publisher.publish(topic_path, data_bytes)
            message_id = future.result()
            print(f"[{self.agent_name}] Published message to {topic_id} with ID: {message_id}")
            return message_id
        except Exception as e:
            print(f"[{self.agent_name}] Failed to publish message to {topic_id}: {e}")
            # Consider adding more robust error handling / retry logic here
            raise e

    def publish_dashboard_update(self, update_data: dict):
        """
        Publishes an update message to the dashboard-updates-topic.
        """
        try:
            self.publish_message(PUBSUB_TOPIC_DASHBOARD_UPDATES, update_data)
        except Exception as e:
            print(f"[{self.agent_name}] Failed to publish dashboard update: {e}")