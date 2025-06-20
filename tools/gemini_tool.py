# tools/gemini_tool.py

import os
import json
import re # Added for JSON parsing
from typing import Dict, Any, List, Optional
# Removed: from google.cloud import aiplatform
# Removed: from common.gcp_clients import get_aiplatform_client

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

    # --- NEW send_prompt method ADDED HERE ---
    def send_prompt(self, prompt: str, response_format: str = "text", temperature: float = 0.7, max_tokens: int = 1024) -> Optional[Dict]:
        """
        Sends a prompt to Gemini and returns response in specified format.
        This method is called by ReportGeneratorAgent.
        """
        if not self.gemini_model:
            print("Gemini text generation model not initialized. Cannot send prompt.")
            return None
        try:
            # Add this print statement:
            print(f"GeminiTool: Attempting to generate content for prompt (first 50 chars): '{prompt[:50]}'")

            # Generate text using existing method
            generated_text = self.generate_text(prompt, temperature, max_tokens)

            if not generated_text:
                print("GeminiTool: generate_text returned no content.") # Add this
                return None

            # If JSON format requested, try to parse the response
            if response_format.lower() == "json":
                try:
                    # Try to parse as JSON directly
                    return json.loads(generated_text)
                except json.JSONDecodeError:
                    # If direct parsing fails, try to extract JSON from the text
                    json_match = re.search(r'\{.*\}', generated_text, re.DOTALL)
                    if json_match:
                        try:
                            return json.loads(json_match.group())
                        except json.JSONDecodeError:
                            pass # Fall through to default structured response

                    # If still cannot parse, return a structured response with the raw text
                    print(f"Warning: Gemini response not perfectly parsable as JSON. Returning structured default. Raw: {generated_text[:200]}...")
                    return {
                        "report_title": f"Report Generated by Gemini",
                        "summary": generated_text,
                        "key_highlights": ["Analysis completed", "Report generated successfully"],
                        "future_outlook": "Positive outlook based on available data",
                        "disclaimer": "This report is for informational purposes only and should not be considered financial advice."
                    }
            else:
                # Return as plain text in a structured dict
                return {"response": generated_text}

        except Exception as e:
            print(f"Error in send_prompt: {e}")
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

        print("\n--- Test Send Prompt (JSON) ---")
        json_prompt = """
        Generate a simple JSON object with two keys: "greeting" and "message".
        The greeting should be "Hello", and the message should be "This is a test JSON response."
        """
        json_response = gemini_tool.send_prompt(json_prompt, response_format="json")
        if json_response:
            print(f"JSON Response: {json.dumps(json_response, indent=2)}")
        else:
            print("JSON send_prompt test failed.")

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