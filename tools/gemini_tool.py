import os
import time
from typing import List, Dict, Any, Optional
from google.cloud import aiplatform
from common.gcp_clients import get_vertex_ai_client  # This will initialize aiplatform.init()
from common.constants import PROJECT_ID, REGION, GEMINI_MODEL_NAME, TEXT_EMBEDDING_MODEL_NAME


class GeminiTool:
    """
    A wrapper for Vertex AI Gemini models for text generation and embeddings.
    """

    def __init__(self):
        # Initialize Vertex AI client (sets project and region for subsequent calls)
        self.aiplatform_client = get_vertex_ai_client()
        self.gemini_model = self.aiplatform_client.preview.generative_models.GenerativeModel(GEMINI_MODEL_NAME)
        self.embedding_model = self.aiplatform_client.TextEmbeddingModel.from_pretrained(TEXT_EMBEDDING_MODEL_NAME)

    def generate_text(self, prompt: str, **kwargs) -> Optional[str]:
        """
        Generates text using the configured Gemini model.
        Args:
            prompt: The input prompt for the LLM.
            **kwargs: Additional parameters for model.generate_content (e.g., temperature, max_output_tokens).
        Returns:
            The generated text string, or None if an error occurs.
        """
        try:
            # For Gemini 1.5 Flash, the response structure is typically content.text
            response = self.gemini_model.generate_content(prompt, **kwargs)
            return response.candidates[0].content.parts[0].text
        except Exception as e:
            print(f"Error generating text with Gemini: {e}")
            return None

    def generate_embeddings(self, texts: List[str]) -> Optional[List[List[float]]]:
        """
        Generates text embeddings for a list of texts using Vertex AI Embedding API.
        Args:
            texts: A list of strings for which to generate embeddings. Max 20 texts per request.
        Returns:
            A list of lists of floats, where each inner list is an embedding vector.
            Returns None if an error occurs.
        """
        if not texts:
            return []

        # Vertex AI embedding API has a limit of 20 texts per request.
        # Batching is recommended for larger lists.
        # For simplicity, we'll handle small batches, but for production, implement robust batching.
        max_batch_size = 20
        all_embeddings = []
        for i in range(0, len(texts), max_batch_size):
            batch_texts = texts[i: i + max_batch_size]
            try:
                embeddings = self.embedding_model.get_embeddings(batch_texts)
                for embedding_obj in embeddings:
                    all_embeddings.append(embedding_obj.values)
                # Add a small delay to avoid hitting rate limits too quickly, if many calls are expected
                time.sleep(0.1)
            except Exception as e:
                print(f"Error generating embeddings for a batch: {e}")
                return None
        return all_embeddings


# Example Usage (for testing purposes)
if __name__ == "__main__":
    # Ensure PROJECT_ID and REGION are set as environment variables or in common.constants
    # For local testing, ensure 'gcloud auth application-default login' has been run
    # or you have appropriate service account key configured.

    # Initialize Vertex AI for the session
    aiplatform.init(project=PROJECT_ID, location=REGION)

    gemini_tool = GeminiTool()

    print(f"\n--- Testing Text Generation with {GEMINI_MODEL_NAME} ---")
    test_prompt = "What are the key financial highlights of a tech company?"
    generated_response = gemini_tool.generate_text(test_prompt, temperature=0.7)
    if generated_response:
        print("Generated Text:")
        print(generated_response[:200] + "...")
    else:
        print("Text generation failed.")

    print(f"\n--- Testing Embedding Generation with {TEXT_EMBEDDING_MODEL_NAME} ---")
    test_texts = [
        "The stock market showed strong performance today.",
        "Inflation concerns are impacting bond yields.",
        "Reliance Industries announced strong quarterly results.",
        "Indian stock market is performing well."
    ]
    embeddings = gemini_tool.generate_embeddings(test_texts)
    if embeddings:
        print(f"Generated {len(embeddings)} embeddings. First embedding dimensions: {len(embeddings[0])}")
        # print("First embedding for first text:", embeddings[0][:10], "...") # Print first 10 dimensions
    else:
        print("Embedding generation failed.")