# file handling
import os

# image handling
import base64

# data processing
import pandas as pd

# AI API
import anthropic


def setup_claude_client():
    """Initialize and return an Anthropic client for Claude API."""
    api_key = os.getenv("CLAUDE_API_KEY")
    if not api_key:
        raise ValueError("CLAUDE_API_KEY not found")
    return anthropic.Anthropic(api_key=api_key)

def extract_img2text(image_path, prompt):
    """Extracts text from an image using Claude API with a given prompt."""
    # Read and encode image as base64
    with open(image_path, "rb") as image_file:
        image_data = base64.b64encode(image_file.read()).decode('utf-8')

    # setup constructor
    client = setup_claude_client()

    # Make API call to Claude with image and prompt
    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=3200,
        messages=[{
            "role": "user",
            "content": [{
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": image_data,
                },
            }, {
                "type": "text",
                "text": prompt
            }],
        }],
    )
    return message


def reorder_columns(df: pd.DataFrame, priority_cols: list[str]) -> pd.DataFrame:
    """
    Reorders DataFrame columns by moving specified columns to the beginning."""
    result_df = df.copy()
    for col in reversed(priority_cols):
        if col in result_df.columns:
            result_df.insert(0, col, result_df.pop(col))
    return result_df
