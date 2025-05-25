import os
import time
import pandas as pd
import logging
from tqdm import tqdm
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def call_chatgpt_with_image(image_url, prompt):
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set")
    model = "gpt-4.1-2025-04-14"
    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}}
                ]
            }
        ]
    )
    return response.choices[0].message.content

def parse_caption_to_columns(caption):
    # Split by newlines, strip whitespace, and pad to 8 fields if needed
    lines = [line.strip() for line in caption.strip().split('\n') if line.strip()]
    while len(lines) < 8:
        lines.append("")
    # If more than 8, join extras into the last field
    if len(lines) > 8:
        lines = lines[:7] + [' '.join(lines[7:])]
    return {
        "state": lines[0],
        "desirability": lines[1],
        "photo_appeal": lines[2],
        "issues": lines[3],
        "desc_match": lines[4],
        "new_price": lines[5],
        "sell_price": lines[6],
        "offer_price": lines[7],
    }

def main(csv_path,
          image_folder,
          id_col="product_id",
          image_url_col="image_url",
          description_col="description",
          price_col="current_price",
          prompt_template=None,
          limit=None):
    os.makedirs(image_folder, exist_ok=True)
    df = pd.read_csv(csv_path, dtype=str)
    captions = []
    # Define the prompt template after loading the CSV
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        if limit and idx >= limit:
            break
        item_id = row.get(id_col, str(idx))
        image_url = row.get(image_url_col)
        description = row.get(description_col, "")
        price = row.get(price_col, "")
        if not image_url:
            logging.warning(f"No image URL for row {idx}")
            captions.append("")
            continue
        # Build image filename
        ext = os.path.splitext(image_url)[-1].split('?')[0]
        if not ext or len(ext) > 5:
            ext = ".jpg"
        image_filename = f"{item_id}{ext}"
        image_path = os.path.join(image_folder, image_filename)
        # Download image
        if not os.path.exists(image_path):
            if "https://" not in image_url:
                continue
        # Format the prompt for this row
        prompt = prompt_template.format(description=description, price=price)
        # Call ChatGPT with the image
        try:
            caption = call_chatgpt_with_image(image_url, prompt)
        except Exception as e:
            logging.error(f"Error calling ChatGPT for row {idx}: {e}")
            caption = "\n".join([""]*8)
        captions.append(caption)
        # Parse and save answers to columns
        parsed = parse_caption_to_columns(caption)
        answer_cols = ["state", "desirability", "photo_appeal", "issues", "desc_match", "new_price", "sell_price", "offer_price"]
        for col in answer_cols:
            df.at[idx, col] = parsed[col]
        df.at[idx, "full_caption"] = caption
        # Save intermediate CSV after each row
        out_csv = "captioned_" + os.path.basename(csv_path)
        df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    logging.info(f"Done")

if __name__ == "__main__":
    main(
        csv_path="yad2_collections_bikes_and_scooters_deep_dive.csv",
        image_folder="images_bikes_and_scooters",
        id_col="product_id",
        image_url_col="image_url",
        description_col="description",
        price_col="current_price",
        limit=None,
        prompt_template = '''
You are a professional bike trader with years of experience. you know every model and seen every trick.
You are on the hunt for your next catch that you can buy cheap and resell.

This is an listing of a bike you saw online.
this is their description:
{description}
and their price is {price} NIS

1. estimate their state (1 to 10).
2. give them your secret desirability score of how easy will it be to resell them (1 to 10).
3. How appealing is the photo (1 to 10)?
4. note any issues you see with them (any dents, scratches, improvised fixes or damages) (short answer, comma separated)
5. does their description match what you see (1 to 10)?
6. how much would a new bike like these cost (price in NIS)?
7. how much would would you sell it for?
8. make an offer to the bidder. (price in NIS)

reply only with the answers to my questions with new line separating between the answers.
for example, your answer and nothing more:

7
9
10
ugly repair on the handlebar, torn saddle
5
3500
1500
800
'''
    ) 