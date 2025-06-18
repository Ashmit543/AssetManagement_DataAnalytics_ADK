import json
import os
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
from common.constants import PROJECT_ID, PUBSUB_TOPIC_DASHBOARD_UPDATES, REGION

class ADKBaseAgent:
    """
    A base class for ADK agents to handle common functionalities like
    Pub/Sub message processing and response publishing.
    This acts as a Flask app to receive Pub/Sub push messages.
    """
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.app = Flask(agent_name)
        self.publisher = pubsub_v1.PublisherClient()

        # Define the route for Pub/Sub push messages
        @self.app.route('/', methods=['POST'])
        def index():
            return self.handle_pubsub_message(request)

        # You might add other common routes here for health checks, etc.
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return "OK", 200

    def handle_pubsub_message(self, request):
        """
        Processes incoming Pub/Sub push messages.
        """
        envelope = request.get_json()
        if not envelope:
            print("No Pub/Sub message received.")
            return jsonify({"status": "error", "message": "No Pub/Sub message received"}), 400

        if not isinstance(envelope, dict) or "message" not in envelope:
            print(f"Invalid Pub/Sub message format: {envelope}")
            return jsonify({"status": "error", "message": "Invalid Pub/Sub message format"}), 400

        pubsub_message = envelope["message"]

        # Decode the Pub/Sub message data
        if "data" in pubsub_message:
            message_data_bytes = pubsub_message["data"]
            try:
                message_data = json.loads(message_data_bytes.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"Error decoding message data: {e}. Raw data: {message_data_bytes.decode('utf-8')}")
                return jsonify({"status": "error", "message": f"Invalid JSON in message data: {e}"}), 400
        else:
            message_data = {}

        print(f"[{self.agent_name}] Received message: {message_data}")

        try:
            self.process_message(message_data)
            return jsonify({"status": "success", "message": "Message processed"}), 200
        except Exception as e:
            print(f"[{self.agent_name}] Error processing message: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    def process_message(self, message_data: dict):
        """
        Abstract method to be implemented by child classes.
        This is where the specific agent logic goes.
        """
        raise NotImplementedError("process_message must be implemented by subclasses.")

    def publish_message(self, topic_id: str, message_data: dict):
        """
        Publishes a message to a given Pub/Sub topic.
        """
        topic_path = self.publisher.topic_path(PROJECT_ID, topic_id)
        message_json = json.dumps(message_data)
        message_bytes = message_json.encode("utf-8")

        try:
            future = self.publisher.publish(topic_path, message_bytes)
            message_id = future.result()
            print(f"[{self.agent_name}] Published message to {topic_id} with ID: {message_id}")
            return message_id
        except Exception as e:
            print(f"[{self.agent_name}] Failed to publish message to {topic_id}: {e}")
            raise

    def publish_dashboard_update(self, update_data: dict):
        """
        A helper to publish updates to the dashboard topic.
        """
        self.publish_message(PUBSUB_TOPIC_DASHBOARD_UPDATES, update_data)

    def run(self, host='0.0.0.0', port=os.environ.get('PORT', 8080)):
        """
        Runs the Flask application.
        """
        self.app.run(host=host, port=port)