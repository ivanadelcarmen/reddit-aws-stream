import praw
import datetime
import requests
from configparser import ConfigParser

def main():
    parser = ConfigParser()
    parser.read('config.conf')

    url = parser['api']['url']

    reddit_credentials = {
            'client_id': parser['reddit']['id'],
            'client_secret': parser['reddit']['secret'],
            'username': parser['reddit']['username'],
            'password': parser['reddit']['password'],
            'user_agent': 'RedditStream/0.0.1'
        }
    
    if not url or '' in reddit_credentials.values():
        raise Exception('Add valid Reddit credentials or API Gateway URL to the .conf file')
    
    # Initialize Reddit instance with credentials
    reddit = praw.Reddit(**reddit_credentials)
    subreddit = reddit.subreddit('politics') # Get the r/politics Subreddit instance

    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            if comment.author.name != 'AutoModerator': # Prevent bot mod comments
                dt_utc = datetime.datetime.fromtimestamp(comment.created_utc, datetime.timezone.utc)
                record = {
                    "timestamp": dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'id': comment.id,
                    'text': comment.body,
                    'comment_karma': comment.author.comment_karma,
                    'is_reddit_mod': comment.author.is_mod
                }

                try:
                    response = requests.post(url, json=record)
                    response.raise_for_status()
                    print(f'Comment sent: {record['id']}')

                except requests.exceptions.RequestException as e:
                    print('Request error: ', e)
                    continue

    except KeyboardInterrupt:
        print("Stream interrupted, closing application...")

    finally:
        print('Stopped successfully')


if __name__ == '__main__':
    main()