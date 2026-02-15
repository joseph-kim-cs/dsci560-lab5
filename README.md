Create a python venv: python -m venv .venv

Activate the virtual environment: .venv/Scripts/activate

Install all required packages: pip install -r requirements.txt

Create a docker container: docker compose up -d

Make a copy of the env, and rename to .env

To run the app: python main.py



Additional data: sample data in the /data folder. 


Scripts: 

bs4 scraper: Scrapes the main subreddit page for posts

comment_scraper: Scrapes for comment data

database_pipeline: routes for bs4 -> mysql