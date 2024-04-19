from transformers import BertTokenizer, BertForSequenceClassification, pipeline
import pandas as pd
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def CryptoBertSentiment(data: pd.DataFrame, col_name: str, batch_size: int = 32) -> pd.DataFrame:
    """
    Perform sentiment analysis on a DataFrame using CryptoBERT.
    """
    try:
        tokenizer = BertTokenizer.from_pretrained("kk08/CryptoBERT")
        model     = BertForSequenceClassification.from_pretrained("kk08/CryptoBERT")
        classifier = pipeline(
            "sentiment-analysis", model=model, tokenizer=tokenizer, truncation=True, padding=True
        )

        sentiments, scores = [], []
        for i in range(0, len(data), batch_size):
            batch = data[col_name].iloc[i:i+batch_size].tolist()
            try:
                batch_results    = classifier(batch)
                batch_sentiments = [res['label'] for res in batch_results]
                batch_scores     = [res['score'] for res in batch_results]
            except Exception as e:
                logging.error(f"Error processing batch starting at index {i}: {e}")
                # Fallback values in case of error to avoid breaking the pipeline
                batch_sentiments = ['Unknown'] * len(batch)
                batch_scores     = [0.0] * len(batch)

            sentiments.extend(batch_sentiments)
            scores.extend(batch_scores)
#
        # Map labels and calculate scores
        data['Sentiment'] = pd.Series(sentiments).map(
            {
                'LABEL_1': 'Positive',
                'LABEL_0': 'Negative',
                'Unknown': 'Unknown'
            }
        )
#
        data['Sentiment Score'] = pd.Series(scores)
        data['Negative Score'] = data['Sentiment Score'].where(
            data['Sentiment'] == 'Negative', other=1 - data['Sentiment Score']
        )
        data['Positive Score'] = data['Sentiment Score'].where(
            data['Sentiment'] == 'Positive', other=data['Sentiment Score']
        )

        # display example observation after sentiment analysis to verify the results
        logging.info(f"{99*'~'}\nSample observation after sentiment analysis: {data.iloc[0]}\n{99*'~'}")

    except Exception as e:
        logging.error(f"Failed to complete sentiment analysis: {e}")
        # Option: Return the data unchanged, add an error log, raise an exception, etc.
        # raise exception
        raise e

    return data
#


