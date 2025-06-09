import re
from textblob import TextBlob

def clean_comment(comment: str) -> str:
    # Remove markdown formatting (bold, italics, strikethrough)
    comment = re.sub(r'(\*\*|\*|__|~~)(.*?)\1', r'\2', comment)

    # Remove links
    comment = re.sub(r'\[.*?\]\(.*?\)', '', comment)  # Markdown links
    comment = re.sub(r'http[s]?://\S+', '', comment)  # URLs
    comment = re.sub(r'www\.\S+', '', comment)  # WWW links

    # Remove user and subreddit mentions
    comment = re.sub(r'u/\w+', '', comment)
    comment = re.sub(r'r/\w+', '', comment)

    # Remove flair or tag information (e.g. [INFO]) and blockquotes
    comment = re.sub(r'\[.*?\]', '', comment)
    comment = re.sub(r'^>.*$', '', comment, flags=re.MULTILINE)

    # Remove extra spaces and newlines
    comment = re.sub(r'\s+', ' ', comment).strip()

    return comment


def transform_message(msg: dict, body_attribute: str) -> dict:
    """
    Builds the transformed message with clean comment text and sentiment analysis info.
    
    Args:
        msg: The original JSON message from the topic parsed into dict type.
        body_attribute: The attribute of the dict object where the comment body is stored.

    Returns:
        dict: A copy of the original message processed according to the specification.
    """
    try:
        msg_copy = dict(msg) # Do not modify the original message by working over a copy

        cleaned_text = clean_comment(msg_copy[body_attribute])
        polarity_score = TextBlob(cleaned_text).sentiment.polarity
        subjectivity_score = TextBlob(cleaned_text).sentiment.subjectivity

        def get_sentiment(score: float) -> str:
            if score < 0:
                return 'negative'
            elif score > 0:
                return 'positive'
            else:
                return 'neutral'
        
        analysis = {
            'polarity': polarity_score,
            'subjectivity': subjectivity_score,
            'sentiment': get_sentiment(polarity_score)
        }

        msg_copy[body_attribute] = cleaned_text # Replace the old comment body with the formatted one
        msg_copy.update(analysis) # Add sentiment analysis data to the structure
        return msg_copy

    except AttributeError:
        print(f"Attribute '{body_attribute}' is not available in the message schema")