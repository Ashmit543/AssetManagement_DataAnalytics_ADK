# tools/gemini_tool.py

import os
import json
from typing import Dict, Any, List, Optional
from google.cloud import aiplatform
from common.gcp_clients import get_aiplatform_client

from common.constants import PROJECT_ID, REGION, GEMINI_MODEL_NAME, TEXT_EMBEDDING_MODEL_NAME

# NEW IMPORTS FOR GEMINI MODELS
import vertexai
from vertexai.generative_models import GenerativeModel, Part, Content, HarmCategory, HarmBlockThreshold
# NEW IMPORT FOR EMBEDDING MODEL
from vertexai.language_models import TextEmbeddingModel  # Import TextEmbeddingModel


class GeminiTool:
    """
    A tool for interacting with Google's Gemini large language models via Vertex AI.
    """

    def __init__(self):
        vertexai.init(project=PROJECT_ID, location=REGION)

        try:
            self.gemini_model = GenerativeModel(GEMINI_MODEL_NAME)

            # Instantiate TextEmbeddingModel separately
            self.embedding_model = TextEmbeddingModel.from_pretrained(TEXT_EMBEDDING_MODEL_NAME)

            print(f"GeminiTool initialized with text generation model: {GEMINI_MODEL_NAME} in {REGION}.")
            print(f"GeminiTool initialized with embedding model: {TEXT_EMBEDDING_MODEL_NAME} in {REGION}.")
            print("Gemini models loaded successfully.")

        except Exception as e:
            print(f"Error initializing Gemini models: {e}")
            print("Please ensure GEMINI_MODEL_NAME and TEXT_EMBEDDING_MODEL_NAME are correct in common/constants.py,")
            print("and your service account has 'Vertex AI User' role.")
            self.gemini_model = None
            self.embedding_model = None

    def generate_text(self, prompt: str, temperature: float = 0.7, max_tokens: int = 1024) -> Optional[str]:
        """
        Generates text using the configured Gemini model.
        """
        if not self.gemini_model:
            print("Gemini text generation model not initialized. Cannot generate text.")
            return None

        try:
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }

            response = self.gemini_model.generate_content(
                prompt,
                generation_config={
                    "temperature": temperature,
                    "max_output_tokens": max_tokens
                },
                safety_settings=safety_settings
            )

            if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
                return response.candidates[0].content.parts[0].text
            else:
                print(f"Gemini response did not contain expected text content or was blocked. Prompt: '{prompt}'")
                if response.prompt_feedback and response.prompt_feedback.block_reason:
                    print(f"Block Reason: {response.prompt_feedback.block_reason}")
                return None
        except Exception as e:
            print(f"Error generating text with Gemini: {e}")
            return None

    def get_text_embedding(self, text: str) -> Optional[List[float]]:
        """
        Generates a text embedding for the given text.
        """
        if not self.embedding_model:
            print("Embedding model not initialized. Cannot get embedding.")
            return None

        try:
            # CORRECTED: Call get_embeddings() method on TextEmbeddingModel
            # The get_embeddings() method for embedding models takes a list of texts
            embeddings = self.embedding_model.get_embeddings([text])

            # Access the embedding values, typically from the first embedding object
            if embeddings and embeddings[0].values:  # embeddings is a list of Embedding objects
                return embeddings[0].values
            else:
                print(f"Embedding response did not contain expected values. Text: '{text}' Response: {embeddings}")
                return None
        except Exception as e:
            print(f"Error generating embedding with Gemini: {e}")
            return None


# Example Usage
if __name__ == "__main__":
    print("--- Running GeminiTool tests ---")
    gemini_tool = GeminiTool()

    if gemini_tool.gemini_model:
        print("\n--- Test Text Generation ---")
        prompt = "What is the capital of France?"
        generated_text = gemini_tool.generate_text(prompt)
        if generated_text:
            print(f"Prompt: {prompt}")
            print(f"Generated Text: {generated_text}")
        else:
            print("Text generation failed.")

    if gemini_tool.embedding_model:
        print("\n--- Test Text Embedding ---")
        text_to_embed = "Generative AI is transforming industries."
        embedding = gemini_tool.get_text_embedding(text_to_embed)
        if embedding:
            print(f"Text: '{text_to_embed}'")
            print(f"Embedding (first 5 values): {embedding[:5]}...")
            print(f"Embedding length: {len(embedding)}")
        else:
            print("Embedding generation failed.")
    else:
        print("Gemini Tool not initialized properly, skipping tests.")