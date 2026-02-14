# we will use this to invoke the main function of our application
from scripts.reddit_scraper import main
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

if __name__ == "__main__":
    main()
