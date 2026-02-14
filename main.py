# we will use this to invoke the main function of our application
from scripts.reddit_scraper import fetch_posts
import argparse
from dotenv import load_dotenv
import os
import praw

load_dotenv()

# client credentials are loaded from environment variables for security and flexibility
client_id = os.getenv("REDDIT_CLIENT_ID")
client_secret = os.getenv("REDDIT_CLIENT_SECRET")
user_agent = os.getenv("REDDIT_USER_AGENT")

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent=user_agent,
    check_for_async=False,  # helps avoid async warnings in some environments
)

def main():
    parser = argparse.ArgumentParser(description="Reddit Scraper")
    parser.add_argument("--subreddit", required=True, help="e.g., tech, cybersecurity")
    parser.add_argument("--limit", type=int, default=200, help="number of posts to fetch")
    parser.add_argument("--sort", choices=["new", "hot", "top"], default="new")
    parser.add_argument("--batch", type=int, default=500, help="API batch size per call")
    args = parser.parse_args()

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        check_for_async=False,  # helps avoid async warnings in some environments
    )
    posts = fetch_posts(
        reddit=reddit,
        subreddit_name=args.subreddit,
        total_limit=args.limit,
        sort=args.sort,
        batch_size=args.batch,
    )

    print(f"Fetched {len(posts)} posts from r/{args.subreddit}. Sample:")
    for p in posts[:3]:
        print("-", p["created_iso"], p["title"][:80])


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
