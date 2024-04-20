from datetime import datetime, timedelta
import time
from typing import List
import pandas as pd
import numpy as np
import praw
from typing import List

#
def fetch_subreddit_data(
    authentication: dict,
    subreddit_name: str,
    sort: str,
    months_back: int,
    requests_per_minute: int,
    limit_per_request: int,
    keywords_list: List[str]
    ) -> pd.DataFrame:
    """
    Fetches data from a subreddit based on the given parameters.
    """
    try:
        authentication = praw.Reddit(
            client_id     = authentication['REDDIT_CLIENT_ID'],
            client_secret = authentication['REDDIT_CLIENT_SECRET'],
            user_agent    = authentication['REDDIT_USER_AGENT'],
            username      = authentication['REDDIT_USERNAME'],
            password      = authentication['REDDIT_PASSWORD']
        )
        authentication.user.me()
    except praw.exceptions.PRAWException as e:  # Adjust the exception type as needed
        raise ValueError("Failed to authenticate with Reddit API.") from e
    else:
        print("API connection established successfully.")

    # Fetch subreddit data
    subreddit = authentication.subreddit(subreddit_name)
    end_date = datetime.utcnow() - timedelta(days=30 * months_back)
    submission_generator = getattr(subreddit, sort)(limit=limit_per_request)
    posts_dict = {
        "ID": [], "Author": [], "Title": [],  "Body": [], "Score": [], "Total Comments": [], "Votes": [], "URL": [], "Date": []
    }
    request_count = 0

    for submission in submission_generator:
        submission_date = datetime.utcfromtimestamp(submission.created_utc)
        if submission_date < end_date:
            break  # Stop if the post is older than n months

        # Check if the post contains any of the keywords (do not include the post if it does not contain any of the keywords)
        if not any(
            keyword.lower() in submission.title.lower()
            for keyword in keywords_list
        ):
            continue

        # Append post data to the dictionary
        posts_dict["ID"].append(submission.id)
        posts_dict["Author"].append(submission.author.name if submission.author else 'Deleted')
        posts_dict["Title"].append(submission.title)
        posts_dict["Body"].append(submission.selftext if submission.selftext else "")
        posts_dict["Score"].append(submission.score)
        posts_dict["Total Comments"].append(submission.num_comments)
        posts_dict["Votes"].append(submission.upvote_ratio)
        posts_dict["URL"].append(submission.url)
        posts_dict["Date"].append(submission_date.strftime('%Y-%m-%d'))
        request_count += 1 # Increment the request count
        if request_count >= requests_per_minute:
            time.sleep(60)  # Sleep to adhere to the rate limit
            request_count = 0  # Reset the request count

    return pd.DataFrame(posts_dict)